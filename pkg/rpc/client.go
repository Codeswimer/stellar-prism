// Package rpc defines the narrow view of Soroban RPC that prism depends on,
// and an adapter over the Stellar SDK client that implements it.
//
// The Client interface exists so the streaming state machine can be tested
// without a network. Everything the streaming core does against RPC goes
// through this interface, which keeps the fake in internal/fakerpc small
// enough to reason about.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// DefaultURL is the public testnet endpoint, so prism works with no
// configuration. Mainnet and local networks are selected with --rpc-url or
// SOROBAN_RPC_URL.
const DefaultURL = "https://soroban-testnet.stellar.org"

// URLEnvVar is the environment variable consulted when --rpc-url is not given.
const URLEnvVar = "SOROBAN_RPC_URL"

// HeadersEnvVar is the environment variable consulted for custom HTTP headers
// attached to outgoing RPC requests. Entries may be separated by newlines or
// commas and use the same "Key: Value" format as --header.
const HeadersEnvVar = "PRISM_HEADERS"

// Client is the subset of Soroban RPC that prism uses.
//
// It is deliberately tiny: read methods, all taking a context. New
// stream sources should prefer composing these over widening the interface,
// because every method added here must also be faked in tests.
type Client interface {
	// GetEvents returns contract events matching the request. Either
	// StartLedger or Pagination.Cursor must be set, never both — the RPC
	// rejects requests carrying both.
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)

	// GetEventsBatch returns contract events matching each request, combining
	// them into as few HTTP roundtrips as the server allows. Each entry's
	// StartLedger or Pagination.Cursor must be mutually exclusive; at least
	// one must be set.
	//
	// A failed entry aborts the batch: the responses gathered before the
	// failure are returned alongside the error, so a caller never has to
	// guess how far the batch got.
	GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error)

	// GetLedgers returns ledger metadata starting at the requested point.
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)

	// GetTransaction returns a single transaction by hash.
	GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error)

	// GetTransactions returns transactions matching the request.
	GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error)

	// SimulateTransaction estimates resource usage and authorization for a
	// transaction envelope without submitting it.
	SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error)

	// GetLatestLedger reports the head of the chain, which is how a stream
	// with no explicit start ledger works out where "now" is.
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// ResolveURL picks a single RPC endpoint from, in order of precedence: an
// explicit flag value, the SOROBAN_RPC_URL environment variable, then
// DefaultURL.
//
// It is kept for callers that only support one endpoint; ResolveURLs is the
// form used by the CLI, where a failover pool may be configured.
func ResolveURL(flagValue string) string {
	return ResolveURLs([]string{flagValue})[0]
}

// ResolveURLs picks the RPC endpoints from, in order of precedence: explicit
// flag values, the SOROBAN_RPC_URL environment variable, then DefaultURL.
//
// Multiple endpoints may be configured by repeating --rpc-url, or by
// separating them with commas in SOROBAN_RPC_URL. Blank entries are ignored.
func ResolveURLs(flagValues []string) []string {
	urls := cleanURLs(flagValues)
	if len(urls) > 0 {
		return urls
	}
	if env := strings.TrimSpace(os.Getenv(URLEnvVar)); env != "" {
		if urls := cleanURLs(strings.Split(env, ",")); len(urls) > 0 {
			return urls
		}
	}
	return []string{DefaultURL}
}

func cleanURLs(values []string) []string {
	urls := make([]string, 0, len(values))
	for _, value := range values {
		if url := strings.TrimSpace(value); url != "" {
			urls = append(urls, url)
		}
	}
	return urls
}

// ResolveSRV resolves the DNS SRV records for domain (the _rpc._tcp.<domain>
// records published by a clustered RPC deployment) and returns one https URL
// per advertised node. It is how a fleet of prism instances can share a
// single hostname and still spread across the cluster behind it.
func ResolveSRV(domain string) ([]string, error) {
	_, records, err := net.LookupSRV("rpc", "tcp", domain)
	if err != nil {
		return nil, fmt.Errorf("srv lookup for %s: %w", domain, err)
	}

	urls := make([]string, 0, len(records))
	for _, srv := range records {
		// SRV targets are absolute names, so they carry a trailing dot that
		// must be trimmed before the host is used in a URL.
		target := strings.TrimSuffix(srv.Target, ".")
		urls = append(urls, fmt.Sprintf("https://%s:%d", target, srv.Port))
	}
	return urls, nil
}

// SDKClient adapts the Stellar SDK's RPC client to the Client interface.
type SDKClient struct {
	url        string
	httpClient *http.Client
	inner      *rpcclient.Client
}

// RpcRequest represents a single JSON-RPC 2.0 request item in a batch call.
type RpcRequest struct {
	JSONRPC string `json:"jsonrpc,omitempty"`
	ID      any    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// RpcError represents a JSON-RPC 2.0 error object returned for a batch item.
type RpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements error.
func (e *RpcError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message)
}

// RpcResponse represents a single JSON-RPC 2.0 response item correlated by ID.
type RpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RpcError       `json:"error,omitempty"`
}

// New returns a Client backed by the Stellar SDK, talking to url.
//
// The HTTP client carries a per-request timeout so a hung connection surfaces
// as a retryable error rather than stalling the stream indefinitely.
func New(url string, timeout time.Duration) *SDKClient {
	return NewWithTransport(url, timeout, nil)
}

// NewWithTransport returns a Client backed by the Stellar SDK that sends every
// HTTP request through transport.
//
// A nil transport means http.DefaultTransport, so New and NewWithTransport
// behave identically when no transport is supplied. A *WireLogger is the
// intended use: it observes the exchange while remaining transparent to the
// streaming code and the SDK.
func NewWithTransport(url string, timeout time.Duration, transport http.RoundTripper) *SDKClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if _, ok := transport.(*rateLimitTransport); !ok {
		transport = NewRateLimitTransport(transport, RateLimitConfig{})
	}
	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	return &SDKClient{
		url:        url,
		httpClient: httpClient,
		inner:      rpcclient.NewClient(url, httpClient),
	}
}

// NewWithHeaders returns a Client backed by the Stellar SDK that attaches
// headers to every outgoing HTTP request.
func NewWithHeaders(url string, timeout time.Duration, headers http.Header) *SDKClient {
	return NewWithOptions(url, timeout, NewClientOption{Headers: headers})
}

// NewWithOptions returns a Client backed by the Stellar SDK configured with opts.
func NewWithOptions(url string, timeout time.Duration, opts ...NewClientOption) *SDKClient {
	applied := applyNewClientOptions(opts...)
	transport := NewHeaderTransport(applied.transport, applied.headers)
	if applied.rateLimit != nil {
		transport = NewRateLimitTransport(transport, *applied.rateLimit)
	}
	return NewWithTransport(url, timeout, transport)
}

// GetEvents implements Client.
func (c *SDKClient) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	resp, err := c.inner.GetEvents(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getEvents: %w", err)
	}
	return resp, nil
}

// BatchCall sends a slice of JSON-RPC 2.0 requests in a single HTTP POST
// array payload and correlates the returned responses by request ID.
//
// Partial failures are preserved: if individual batch items return JSON-RPC
// errors or are omitted by the server, all received responses are returned in
// the input request order alongside a joined error describing the failures.
func (c *SDKClient) BatchCall(ctx context.Context, requests []RpcRequest) (_ []RpcResponse, retErr error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	outbound := make([]RpcRequest, len(requests))
	idKeys := make([]string, len(requests))
	idToIndex := make(map[string]int, len(requests))
	for i, req := range requests {
		if req.Method == "" {
			return nil, Permanent(fmt.Errorf("rpc batch: request %d has empty method", i))
		}
		if req.JSONRPC == "" {
			req.JSONRPC = "2.0"
		}
		if req.ID == nil {
			req.ID = i + 1
		}
		key, err := canonicalRPCID(req.ID)
		if err != nil {
			return nil, Permanent(fmt.Errorf("rpc batch: request %d invalid id: %w", i, err))
		}
		if prev, dup := idToIndex[key]; dup {
			return nil, Permanent(fmt.Errorf("rpc batch: requests %d and %d share duplicate id %s", prev, i, key))
		}
		idToIndex[key] = i
		idKeys[i] = key
		outbound[i] = req
	}

	payload, err := json.Marshal(outbound)
	if err != nil {
		return nil, fmt.Errorf("rpc batch: marshalling requests: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("rpc batch: building request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("rpc batch: %w", err)
	}
	defer func() {
		if closeErr := httpResp.Body.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("rpc batch: closing response body: %w", closeErr)
		}
	}()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("rpc batch: reading response: %w", err)
	}
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		snippet := strings.TrimSpace(string(body))
		if snippet != "" {
			return nil, fmt.Errorf("rpc batch: http %d %s: %s", httpResp.StatusCode, httpResp.Status, snippet)
		}
		return nil, fmt.Errorf("rpc batch: http %d %s", httpResp.StatusCode, httpResp.Status)
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("rpc batch: empty response body")
	}
	if trimmed[0] == '{' {
		var single RpcResponse
		if err := json.Unmarshal(trimmed, &single); err != nil {
			return nil, fmt.Errorf("rpc batch: decoding error object: %w", err)
		}
		if single.Error != nil {
			return nil, fmt.Errorf("rpc batch: %w", single.Error)
		}
		return nil, fmt.Errorf("rpc batch: expected JSON array response, got single object")
	}

	var rawResponses []RpcResponse
	if err := json.Unmarshal(trimmed, &rawResponses); err != nil {
		return nil, fmt.Errorf("rpc batch: decoding response array: %w", err)
	}

	ordered := make([]RpcResponse, len(requests))
	seen := make([]bool, len(requests))
	var itemErrs []error
	for i, resp := range rawResponses {
		key, err := canonicalRawRPCID(resp.ID)
		if err != nil {
			itemErrs = append(itemErrs, fmt.Errorf("response %d has invalid id %s: %w", i, string(resp.ID), err))
			continue
		}
		idx, ok := idToIndex[key]
		if !ok {
			itemErrs = append(itemErrs, fmt.Errorf("unexpected response id %s", key))
			continue
		}
		if seen[idx] {
			itemErrs = append(itemErrs, fmt.Errorf("duplicate response for id %s", key))
			continue
		}
		seen[idx] = true
		ordered[idx] = resp
		if resp.Error != nil {
			itemErrs = append(itemErrs, fmt.Errorf("item %d (id %s): %w", idx, key, resp.Error))
		}
	}

	for idx, ok := range seen {
		if !ok {
			itemErrs = append(itemErrs, fmt.Errorf("missing response for item %d (id %s)", idx, idKeys[idx]))
		}
	}

	if len(itemErrs) > 0 {
		return ordered, errors.Join(itemErrs...)
	}
	return ordered, nil
}

func canonicalRPCID(id any) (string, error) {
	raw, err := json.Marshal(id)
	if err != nil {
		return "", err
	}
	return canonicalRawRPCID(raw)
}

func canonicalRawRPCID(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", errors.New("null or empty id")
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", err
		}
		encoded, err := json.Marshal(s)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
	if n, err := strconv.ParseInt(string(trimmed), 10, 64); err == nil {
		return strconv.FormatInt(n, 10), nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err != nil {
		return "", err
	}
	return compact.String(), nil
}

// GetEventsBatch implements Client, packing all queries into a single JSON-RPC
// 2.0 batch request and returning partial responses up to any failed entry.
func (c *SDKClient) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	rpcReqs := make([]RpcRequest, len(reqs))
	for i, req := range reqs {
		rpcReqs[i] = RpcRequest{
			JSONRPC: "2.0",
			ID:      i + 1,
			Method:  protocol.GetEventsMethodName,
			Params:  req,
		}
	}

	rawResps, batchErr := c.BatchCall(ctx, rpcReqs)
	if len(rawResps) == 0 && batchErr != nil {
		return nil, fmt.Errorf("rpc getEventsBatch: %w", batchErr)
	}

	responses := make([]protocol.GetEventsResponse, 0, len(reqs))
	for i, rawResp := range rawResps {
		if rawResp.Error != nil {
			// Hand back the responses gathered before the failure alongside the error.
			return responses, fmt.Errorf("rpc getEventsBatch: item %d: %w", i, rawResp.Error)
		}
		if len(rawResp.Result) == 0 {
			if batchErr != nil {
				return responses, fmt.Errorf("rpc getEventsBatch: %w", batchErr)
			}
			return responses, fmt.Errorf("rpc getEventsBatch: item %d: missing result", i)
		}
		var decoded protocol.GetEventsResponse
		if err := json.Unmarshal(rawResp.Result, &decoded); err != nil {
			return responses, fmt.Errorf("rpc getEventsBatch: decode item %d: %w", i, err)
		}
		responses = append(responses, decoded)
	}
	if batchErr != nil {
		return responses, fmt.Errorf("rpc getEventsBatch: %w", batchErr)
	}
	return responses, nil
}

// GetLedgers implements Client.
func (c *SDKClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	resp, err := c.inner.GetLedgers(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgers: %w", err)
	}
	return resp, nil
}

// GetLedgerEntries returns the requested ledger entries.
//
// It is deliberately not part of Client: the streaming core only needs the
// core methods above, and keeping the interface small avoids forcing every
// test double to implement a method it never uses. Callers that need it, such
// as contract ABI resolution, assert for LedgerEntriesClient instead.
func (c *SDKClient) GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	resp, err := c.inner.GetLedgerEntries(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgerEntries: %w", err)
	}
	return resp, nil
}

// LedgerEntriesClient is the optional capability used to resolve contract
// specs from on-chain WASM. SDKClient implements it.
type LedgerEntriesClient interface {
	GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error)
}

// GetTransaction implements Client.
func (c *SDKClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	resp, err := c.inner.GetTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransaction: %w", err)
	}
	return resp, nil
}

// GetTransactions implements Client.
func (c *SDKClient) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	resp, err := c.inner.GetTransactions(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransactions: %w", err)
	}
	return resp, nil
}

// SimulateTransaction implements Client.
func (c *SDKClient) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	resp, err := c.inner.SimulateTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc simulateTransaction: %w", err)
	}
	return resp, nil
}

// GetLatestLedger implements Client.
func (c *SDKClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	resp, err := c.inner.GetLatestLedger(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getLatestLedger: %w", err)
	}
	return resp, nil
}

// GetNetwork implements NetworkClient.
func (c *SDKClient) GetNetwork(ctx context.Context) (protocol.GetNetworkResponse, error) {
	resp, err := c.inner.GetNetwork(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getNetwork: %w", err)
	}
	return resp, nil
}

// GetHealth implements HealthClient. It is intentionally not part of the
// streaming Client interface; only the health command needs it.
func (c *SDKClient) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	resp, err := c.inner.GetHealth(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getHealth: %w", err)
	}
	return resp, nil
}

// Close releases the underlying HTTP resources.
func (c *SDKClient) Close() error {
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("rpc close: %w", err)
	}
	return nil
}

// NewClientOption configures optional behaviour of the SDK-backed client.
type NewClientOption struct {
	// ProxyURL, when set, routes RPC traffic through an HTTP, HTTPS or SOCKS5
	// proxy instead of dialling the endpoint directly.
	ProxyURL string

	// Headers, when non-empty, are attached to every outgoing RPC HTTP request.
	Headers http.Header

	// Transport, when set, overrides the base HTTP round tripper.
	Transport http.RoundTripper

	// RateLimit, when set, configures client-side token-bucket throttling and
	// HTTP 429 Retry-After backoff on the HTTP transport.
	RateLimit *RateLimitConfig
}

// newClientOptions collects options for SDK client creation.
type newClientOptions struct {
	transport http.RoundTripper
	headers   http.Header
	rateLimit *RateLimitConfig
}

// applyNewClientOptions resolves opts into the concrete transport settings
// used to build the client. An unusable option is reported and ignored rather
// than fatal: a mistyped proxy should degrade to a direct connection, not
// leave prism unable to start.
func applyNewClientOptions(opts ...NewClientOption) newClientOptions {
	applied := newClientOptions{}
	for _, opt := range opts {
		if opt.Transport != nil {
			applied.transport = opt.Transport
		}
		if opt.ProxyURL != "" {
			transport, err := proxyURLToTransport(opt.ProxyURL)
			if err != nil {
				fmt.Fprintf(os.Stderr, "rpc: ignoring proxy URL %q: %v\n", opt.ProxyURL, err)
			}
			if err == nil {
				applied.transport = transport
			}
		}
		if len(opt.Headers) > 0 {
			if applied.headers == nil {
				applied.headers = make(http.Header, len(opt.Headers))
			}
			mergeHeaders(applied.headers, opt.Headers)
		}
		if opt.RateLimit != nil {
			cfg := *opt.RateLimit
			applied.rateLimit = &cfg
		}
	}
	return applied
}

// ParseHeaders parses "Key: Value" header strings into an http.Header.
// Blank entries are ignored. Header names and values are validated to reject
// malformed syntax and control characters.
func ParseHeaders(raw []string) (http.Header, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	parsed := make(http.Header, len(raw))
	for _, entry := range raw {
		if strings.TrimSpace(entry) == "" {
			continue
		}
		key, val, err := parseHeader(entry)
		if err != nil {
			return nil, err
		}
		parsed.Add(key, val)
	}
	if len(parsed) == 0 {
		return nil, nil
	}
	return parsed, nil
}

// ResolveHeaders resolves custom HTTP headers from explicit CLI flag values
// and the PRISM_HEADERS environment variable. When both define the same header
// name, the explicit flag value overrides the environment value.
func ResolveHeaders(flagValues []string) (http.Header, error) {
	var envHeaders http.Header
	if env := strings.TrimSpace(os.Getenv(HeadersEnvVar)); env != "" {
		var err error
		envHeaders, err = ParseHeaders(splitEnvHeaders(env))
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", HeadersEnvVar, err)
		}
	}

	flagHeaders, err := ParseHeaders(flagValues)
	if err != nil {
		return nil, err
	}

	if len(envHeaders) == 0 {
		return flagHeaders, nil
	}
	if len(flagHeaders) == 0 {
		return envHeaders, nil
	}

	merged := envHeaders.Clone()
	mergeHeaders(merged, flagHeaders)
	return merged, nil
}

// NewHeaderTransport wraps base so every outgoing HTTP request carries headers.
// When headers is empty, base is returned unchanged.
func NewHeaderTransport(base http.RoundTripper, headers http.Header) http.RoundTripper {
	cloned := cloneCleanHeaders(headers)
	if len(cloned) == 0 {
		return base
	}
	return &headerTransport{
		base:    base,
		headers: cloned,
	}
}

type headerTransport struct {
	base    http.RoundTripper
	headers http.Header
}

// RoundTrip implements http.RoundTripper, cloning req before attaching custom
// headers so the caller's request is never mutated.
func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("rpc: nil request")
	}
	cloned := req.Clone(req.Context())
	if cloned.Header == nil {
		cloned.Header = make(http.Header, len(t.headers))
	}
	mergeHeaders(cloned.Header, t.headers)

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(cloned)
}

func mergeHeaders(dst, src http.Header) {
	for key, values := range src {
		if len(values) == 0 {
			continue
		}
		dst[http.CanonicalHeaderKey(key)] = slices.Clone(values)
	}
}

func cloneCleanHeaders(src http.Header) http.Header {
	if len(src) == 0 {
		return nil
	}
	dst := make(http.Header, len(src))
	mergeHeaders(dst, src)
	if len(dst) == 0 {
		return nil
	}
	return dst
}

func parseHeader(raw string) (string, string, error) {
	key, val, ok := strings.Cut(raw, ":")
	if !ok {
		return "", "", fmt.Errorf("invalid header %q: expected Key: Value", raw)
	}
	key = strings.TrimSpace(key)
	if !isValidHeaderName(key) {
		return "", "", fmt.Errorf("invalid header name %q", key)
	}
	val = strings.TrimSpace(val)
	if !isValidHeaderValue(val) {
		return "", "", fmt.Errorf("invalid header value for %q", key)
	}
	return http.CanonicalHeaderKey(key), val, nil
}

// splitEnvHeaders splits PRISM_HEADERS on newlines and on commas that precede a
// new "Key: Value" entry, preserving commas inside header values such as Accept
// or Date.
func splitEnvHeaders(env string) []string {
	var out []string
	for _, line := range strings.Split(env, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" {
			continue
		}
		out = append(out, splitCommaHeaders(line)...)
	}
	return out
}

func splitCommaHeaders(line string) []string {
	var out []string
	start := 0
	for i := 0; i < len(line); i++ {
		if line[i] != ',' {
			continue
		}
		rest := strings.TrimSpace(line[i+1:])
		if rest == "" {
			if seg := strings.TrimSpace(line[start:i]); seg != "" {
				out = append(out, seg)
			}
			return out
		}
		if name, _, hasColon := strings.Cut(rest, ":"); hasColon && isValidHeaderName(strings.TrimSpace(name)) {
			if seg := strings.TrimSpace(line[start:i]); seg != "" {
				out = append(out, seg)
			}
			start = i + 1
		}
	}
	if seg := strings.TrimSpace(line[start:]); seg != "" {
		out = append(out, seg)
	}
	return out
}

func isValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenChar(name[i]) {
			return false
		}
	}
	return true
}

func isTokenChar(c byte) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

func isValidHeaderValue(val string) bool {
	for i := 0; i < len(val); i++ {
		b := val[i]
		if b != '\t' && (b < 0x20 || b == 0x7f) {
			return false
		}
	}
	return true
}

// proxyURLToTransport converts a proxy URL into the transport that uses it.
// net/http speaks HTTP, HTTPS and SOCKS5 proxies natively, so the proxy only
// has to be handed to the transport rather than reimplemented.
func proxyURLToTransport(raw string) (http.RoundTripper, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}
	switch u.Scheme {
	case "http", "https", "socks5":
		if u.Host == "" {
			return nil, fmt.Errorf("proxy URL %q has no host", raw)
		}
		return &http.Transport{Proxy: http.ProxyURL(u)}, nil
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q, use http, https, or socks5", u.Scheme)
	}
}

// PermanentError marks a failure that retrying cannot fix, such as a
// malformed filter or a cursor outside the server's retention window.
//
// The streaming loop retries anything not marked permanent, so misclassifying
// a permanent error merely wastes backoff attempts, while misclassifying a
// transient one aborts a stream that would have recovered. When in doubt,
// leave it transient.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the streaming loop stops rather than retrying.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err should abort the stream instead of being
// retried.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// IsRetryable reports whether err looks like a transient condition worth
// retrying: a dropped connection, a timeout, or a server-side hiccup.
//
// A context cancellation is never retryable — that is the operator asking us
// to stop.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if IsPermanent(err) {
		return false
	}
	// Network-level failures are the common case and are always worth a retry.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// The SDK surfaces JSON-RPC and transport errors as plain strings, so fall
	// back to matching the shapes that indicate a server or connection problem
	// rather than a malformed request.
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"eof",
		"timeout",
		"timed out",
		"temporary failure",
		"no such host",
		"server error",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"too many requests",
		"502", "503", "504", "429",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

const (
	defaultRateLimitMaxRetries     = 5
	defaultRateLimitInitialBackoff = 250 * time.Millisecond
	defaultRateLimitMaxBackoff     = 30 * time.Second
)

// RateLimitConfig configures client-side token-bucket throttling and adaptive
// HTTP 429 Retry-After backoff for outgoing RPC requests.
type RateLimitConfig struct {
	// RequestsPerSecond caps outgoing RPC requests per second. Zero or
	// negative disables proactive throttling while keeping HTTP 429 backoff.
	RequestsPerSecond float64
	// Burst sets the token bucket capacity. When <= 0 and RequestsPerSecond > 0,
	// it defaults to max(1, ceil(RequestsPerSecond)).
	Burst int
	// MaxRetries bounds consecutive HTTP 429 retries inside RoundTrip.
	// Zero uses defaultRateLimitMaxRetries (5); negative disables retrying.
	MaxRetries int
	// InitialBackoff and MaxBackoff bound exponential backoff with jitter when
	// an HTTP 429 response does not include a valid Retry-After header.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// Now, Sleep, and Rand are optional hooks for deterministic testing.
	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error
	Rand  func() float64
}

// TokenBucket is a thread-safe token bucket limiter that also coordinates
// shared cooldown windows triggered by HTTP 429 Retry-After responses.
type TokenBucket struct {
	mu           sync.Mutex
	rps          float64
	burst        float64
	tokens       float64
	last         time.Time
	blockedUntil time.Time
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error
}

// NewTokenBucket constructs a TokenBucket permitting rps requests per second
// with the given burst capacity. Non-positive rps disables token throttling
// while preserving PauseFor cooldown support.
func NewTokenBucket(rps float64, burst int, now func() time.Time, sleep func(context.Context, time.Duration) error) *TokenBucket {
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = sleepWithContext
	}
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		rps = 0
	}
	if rps > 0 && burst <= 0 {
		burst = max(1, int(math.Ceil(rps)))
	}
	return &TokenBucket{
		rps:    rps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now(),
		now:    now,
		sleep:  sleep,
	}
}

// Wait blocks until any active 429 cooldown has elapsed and, if rate limiting
// is configured, a token is available, or until ctx is canceled.
func (b *TokenBucket) Wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		now := b.now()
		if b.blockedUntil.After(now) {
			wait := b.blockedUntil.Sub(now)
			b.mu.Unlock()
			if err := b.sleep(ctx, wait); err != nil {
				return err
			}
			continue
		}

		if b.rps <= 0 {
			b.mu.Unlock()
			return nil
		}

		if elapsed := now.Sub(b.last); elapsed > 0 {
			b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.rps)
			b.last = now
		}

		b.tokens--
		if b.tokens >= 0 {
			b.mu.Unlock()
			return nil
		}

		deficit := -b.tokens
		wait := time.Duration(math.Ceil((deficit / b.rps) * float64(time.Second)))
		b.mu.Unlock()

		if wait <= 0 {
			return nil
		}
		return b.sleep(ctx, wait)
	}
}

// PauseFor drains available tokens and pauses all Wait callers for d.
func (b *TokenBucket) PauseFor(d time.Duration) {
	if b == nil || d <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.tokens > 0 {
		b.tokens = 0
	}
	b.last = now
	until := now.Add(d)
	if until.After(b.blockedUntil) {
		b.blockedUntil = until
	}
}

// ParseRetryAfter parses an HTTP Retry-After header value (either non-negative
// integer seconds or an HTTP-date) relative to now.
func ParseRetryAfter(header string, now time.Time) (time.Duration, bool) {
	trimmed := strings.TrimSpace(header)
	if trimmed == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if when, err := http.ParseTime(trimmed); err == nil {
		if !when.After(now) {
			return 0, true
		}
		return when.Sub(now), true
	}
	return 0, false
}

// NewRateLimitTransport wraps base with client-side token-bucket rate limiting
// and adaptive HTTP 429 Retry-After backoff.
func NewRateLimitTransport(base http.RoundTripper, cfg RateLimitConfig) http.RoundTripper {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = sleepWithContext
	}
	randFn := cfg.Rand
	if randFn == nil {
		randFn = rand.Float64
	}
	maxRetries := cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = defaultRateLimitMaxRetries
	} else if maxRetries < 0 {
		maxRetries = 0
	}
	initial := cfg.InitialBackoff
	if initial <= 0 {
		initial = defaultRateLimitInitialBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultRateLimitMaxBackoff
	}
	if maxBackoff < initial {
		maxBackoff = initial
	}

	return &rateLimitTransport{
		base:           base,
		bucket:         NewTokenBucket(cfg.RequestsPerSecond, cfg.Burst, now, sleep),
		maxRetries:     maxRetries,
		initialBackoff: initial,
		maxBackoff:     maxBackoff,
		now:            now,
		rand:           randFn,
	}
}

type rateLimitTransport struct {
	base           http.RoundTripper
	bucket         *TokenBucket
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	now            func() time.Time
	rand           func() float64
}

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("rpc: nil request")
	}

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}

	var bodyBytes []byte
	hasBufferedBody := false
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil && t.maxRetries > 0 {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		closeErr := req.Body.Close()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("rpc rate limit: buffering request body: %w", err), closeErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("rpc rate limit: closing request body: %w", closeErr)
		}
		hasBufferedBody = true
	}

	for attempt := 0; ; attempt++ {
		if err := t.bucket.Wait(req.Context()); err != nil {
			return nil, err
		}

		attemptReq := req.Clone(req.Context())
		if hasBufferedBody {
			attemptReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			attemptReq.ContentLength = int64(len(bodyBytes))
		} else if attempt > 0 && req.GetBody != nil {
			freshBody, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("rpc rate limit: resetting request body: %w", err)
			}
			attemptReq.Body = freshBody
		}

		resp, err := base.RoundTrip(attemptReq)
		if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
			return resp, err
		}
		if attempt >= t.maxRetries {
			return resp, nil
		}

		delay, ok := ParseRetryAfter(resp.Header.Get("Retry-After"), t.now())
		if !ok {
			delay = t.jitteredBackoff(attempt)
		}

		if closeErr := drainAndCloseBody(resp.Body); closeErr != nil {
			return nil, fmt.Errorf("rpc rate limit: draining 429 response: %w", closeErr)
		}

		t.bucket.PauseFor(delay)
	}
}

func (t *rateLimitTransport) jitteredBackoff(attempt int) time.Duration {
	delay := t.initialBackoff
	for i := 0; i < attempt && delay < t.maxBackoff; i++ {
		if delay > t.maxBackoff/2 {
			delay = t.maxBackoff
			break
		}
		delay *= 2
	}
	if delay > t.maxBackoff {
		delay = t.maxBackoff
	}
	half := float64(delay) / 2
	return time.Duration(half + half*t.rand())
}

func drainAndCloseBody(body io.ReadCloser) error {
	if body == nil {
		return nil
	}
	_, copyErr := io.Copy(io.Discard, body)
	closeErr := body.Close()
	return errors.Join(copyErr, closeErr)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
