package main

import (
	"log"
	"os"

	gmapsvc "oldiron/versatilebackend/Gmap/src"
	"oldiron/versatilebackend/internal/app"
)

func main() {
	addr := envOrDefault("GMAP_SERVICE_ADDR", ":8082")
	if err := app.Run(app.Config{
		ServiceName: "gmap-service",
		Addr:        addr,
		Register:    gmapsvc.RegisterRoutes,
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
