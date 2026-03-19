package myipsvc

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Handler struct {
	pool           *Pool
	connectTimeout time.Duration
	idleTimeout    time.Duration
}

func NewHandler(cfg Config) (http.Handler, error) {
	pool, err := NewPool(cfg)
	if err != nil {
		return nil, err
	}
	return &Handler{
		pool:           pool,
		connectTimeout: cfg.ConnectTimeout,
		idleTimeout:    cfg.IdleTimeout,
	}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	key := requestRouteKey(request)
	lease, err := handler.pool.Acquire(key)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadGateway)
		return
	}
	if request.Method == http.MethodConnect {
		ok := handleConnect(writer, request, lease, handler.connectTimeout, handler.idleTimeout, func() {
			handler.pool.MarkFailure(lease)
		})
		if ok {
			handler.pool.MarkSuccess(lease)
		}
		return
	}
	handler.handleHTTP(writer, request, lease)
}

func (handler *Handler) handleHTTP(writer http.ResponseWriter, request *http.Request, lease Lease) {
	outbound := cloneProxyRequest(request)
	response, err := lease.endpoint.client.Do(outbound)
	if err != nil {
		handler.pool.MarkFailure(lease)
		http.Error(writer, err.Error(), http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	handler.pool.MarkSuccess(lease)
	copyResponse(writer, response)
}

func cloneProxyRequest(request *http.Request) *http.Request {
	outbound := request.Clone(request.Context())
	outbound.RequestURI = ""
	outbound.URL = normalizeProxyURL(request)
	stripHopHeaders(outbound.Header)
	outbound.Header.Del("Proxy-Authorization")
	return outbound
}

func normalizeProxyURL(request *http.Request) *url.URL {
	cloned := *request.URL
	if cloned.Scheme == "" {
		cloned.Scheme = "http"
	}
	if cloned.Host == "" {
		cloned.Host = request.Host
	}
	return &cloned
}

func requestRouteKey(request *http.Request) string {
	if strings.TrimSpace(request.Host) != "" {
		return strings.ToLower(strings.TrimSpace(request.Host))
	}
	if request.URL != nil && strings.TrimSpace(request.URL.Host) != "" {
		return strings.ToLower(strings.TrimSpace(request.URL.Host))
	}
	return ""
}

func stripHopHeaders(headers http.Header) {
	for _, key := range []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Proxy-Connection",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		headers.Del(key)
	}
}

func copyResponse(writer http.ResponseWriter, response *http.Response) {
	for key, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}
