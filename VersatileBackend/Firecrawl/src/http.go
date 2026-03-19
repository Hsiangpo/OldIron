package firecrawlsvc

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func buildHTTPClient(proxyURL string, timeoutS int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
	}
	if strings.TrimSpace(proxyURL) != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return &http.Client{
		Timeout:   time.Duration(maxInt(timeoutS, 5)) * time.Second,
		Transport: transport,
	}
}

func readBodyString(reader io.Reader) (string, error) {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func maxInt(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
