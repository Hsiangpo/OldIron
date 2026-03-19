package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	myipsvc "oldiron/versatilebackend/MyIP/src"
)

func main() {
	cfg, err := myipsvc.LoadConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	handler, err := myipsvc.NewHandler(cfg)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           rootHandler(handler),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("myip-service listening on %s", cfg.Addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func rootHandler(proxyHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodConnect && request.URL.Path == "/healthz" {
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(writer).Encode(map[string]string{
				"service": "myip-service",
				"status":  "ok",
			})
			return
		}
		proxyHandler.ServeHTTP(writer, request)
	})
}
