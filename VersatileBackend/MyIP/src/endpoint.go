package myipsvc

import (
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type endpoint struct {
	id            int
	rawURL        string
	proxyURL      *url.URL
	label         string
	client        *http.Client
	failCount     int
	cooldownUntil time.Time
	lastUsedAt    time.Time
}

func newEndpoint(id int, rawURL string, connectTimeout time.Duration) (*endpoint, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" {
		return nil, errors.New("only http upstream proxy is supported")
	}
	if parsed.Hostname() == "" || parsed.Port() == "" {
		return nil, errors.New("upstream proxy host or port is missing")
	}
	return &endpoint{
		id:       id,
		rawURL:   parsed.String(),
		proxyURL: parsed,
		label:    parsed.Host,
		client:   buildEndpointClient(parsed, connectTimeout),
	}, nil
}

func buildEndpointClient(proxyURL *url.URL, connectTimeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout: connectTimeout,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		TLSHandshakeTimeout:   connectTimeout,
		ResponseHeaderTimeout: connectTimeout,
		DisableKeepAlives:     false,
	}
	return &http.Client{
		Transport: transport,
	}
}

func (item *endpoint) proxyAuthorization() string {
	if item.proxyURL.User == nil {
		return ""
	}
	username := item.proxyURL.User.Username()
	password, _ := item.proxyURL.User.Password()
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return "Basic " + token
}
