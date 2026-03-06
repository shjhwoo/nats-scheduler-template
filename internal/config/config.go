package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Load loads environment variables from a .env file.
func Load() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default environment variables")
	}
}

// Get returns the value of an environment variable or a default value.
func Get(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
