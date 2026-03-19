package main

import (
	"log"
	"os"

	"oldiron/versatilebackend/internal/app"
)

func main() {
	addr := envOrDefault("SNOV_SERVICE_ADDR", ":8083")
	if err := app.Run(app.Config{ServiceName: "snov-service", Addr: addr}); err != nil {
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

