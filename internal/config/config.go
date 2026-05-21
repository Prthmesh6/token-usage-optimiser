package config

import "os"

// Config holds gateway runtime settings loaded from the environment.
type Config struct {
	GatewayPort string
	RedisURL    string
	OllamaURL   string
}

// Load reads configuration from environment variables, applying defaults when unset.
func Load() Config {
	return Config{
		GatewayPort: getenv("GATEWAY_PORT", "8080"),
		RedisURL:    getenv("REDIS_URL", "localhost:6379"),
		OllamaURL:   getenv("OLLAMA_URL", "http://localhost:11434"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
