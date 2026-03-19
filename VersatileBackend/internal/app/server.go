package app

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type Config struct {
	ServiceName string
	Addr        string
	Register    func(mux *http.ServeMux)
}

func Run(cfg Config) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(cfg.ServiceName))
	if cfg.Register != nil {
		cfg.Register(mux)
	}
	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("%s listening on %s", cfg.ServiceName, cfg.Addr)
	return server.ListenAndServe()
}

func healthHandler(serviceName string) http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		payload := map[string]string{
			"service": serviceName,
			"status":  "ok",
		}
		_ = json.NewEncoder(writer).Encode(payload)
	}
}
