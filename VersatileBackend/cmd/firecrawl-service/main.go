package main

import (
	"log"
	"os"

	firecrawlsvc "oldiron/versatilebackend/Firecrawl/src"
	"oldiron/versatilebackend/internal/app"
)

func main() {
	addr := envOrDefault("FIRECRAWL_SERVICE_ADDR", ":8081")
	if err := app.Run(app.Config{
		ServiceName: "firecrawl-service",
		Addr:        addr,
		Register:    firecrawlsvc.RegisterRoutes,
	}); err != nil {
		log.Fatal(err)
	}
}

func envOrDefault(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
