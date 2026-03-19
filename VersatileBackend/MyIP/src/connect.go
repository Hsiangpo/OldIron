package myipsvc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func handleConnect(
	writer http.ResponseWriter,
	request *http.Request,
	lease Lease,
	connectTimeout time.Duration,
	idleTimeout time.Duration,
	onFailure func(),
) bool {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		http.Error(writer, "hijack is not supported", http.StatusInternalServerError)
		onFailure()
		return false
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadGateway)
		onFailure()
		return false
	}
	defer clientConn.Close()

	upstreamConn, err := openUpstreamConnection(lease.endpoint.proxyURL, connectTimeout)
	if err != nil {
		_, _ = clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		onFailure()
		return false
	}
	defer upstreamConn.Close()
	_ = upstreamConn.SetDeadline(time.Now().Add(idleTimeout))

	if err := establishTunnel(upstreamConn, request.Host, lease.endpoint.proxyAuthorization()); err != nil {
		_, _ = clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		onFailure()
		return false
	}
	_, _ = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	tunnelConnections(clientConn, upstreamConn)
	return true
}

func openUpstreamConnection(proxyURL *url.URL, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", proxyURL.Host, timeout)
}

func establishTunnel(upstreamConn net.Conn, target string, proxyAuthorization string) error {
	headers := []string{
		fmt.Sprintf("CONNECT %s HTTP/1.1", target),
		fmt.Sprintf("Host: %s", target),
		"Proxy-Connection: Keep-Alive",
	}
	if proxyAuthorization != "" {
		headers = append(headers, "Proxy-Authorization: "+proxyAuthorization)
	}
	headers = append(headers, "", "")
	if _, err := upstreamConn.Write([]byte(strings.Join(headers, "\r\n"))); err != nil {
		return err
	}
	reader := bufio.NewReader(upstreamConn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("upstream proxy connect failed: " + response.Status)
	}
	if reader.Buffered() > 0 {
		return nil
	}
	return nil
}

func tunnelConnections(left net.Conn, right net.Conn) {
	copyDone := make(chan struct{}, 2)
	go copyConn(left, right, copyDone)
	go copyConn(right, left, copyDone)
	<-copyDone
}

func copyConn(dst net.Conn, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	_ = dst.SetDeadline(time.Now())
	_ = src.SetDeadline(time.Now())
	done <- struct{}{}
}
