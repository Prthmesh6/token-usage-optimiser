package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// NewUpstreamProxy returns a reverse proxy targeting the Ollama host. Incoming
// paths (e.g. /v1/chat/completions) are preserved; only scheme and host are rewritten.
func NewUpstreamProxy(ollamaURL string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(ollamaURL)
	if err != nil {
		return nil, fmt.Errorf("proxy: parse ollama URL: %w", err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("proxy: ollama URL must include scheme and host, got %q", ollamaURL)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// No cap on time-to-first-byte — LLM generation can be slow.
		ResponseHeaderTimeout: 0,
	}

	rp := httputil.NewSingleHostReverseProxy(target)
	rp.Transport = transport
	rp.FlushInterval = -1 // flush streaming chunks immediately

	origDirector := rp.Director
	rp.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = target.Host
	}

	return rp, nil
}
