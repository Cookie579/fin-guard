package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	GoogleAPIKey string `json:"google_api_key"`
	GoogleCX     string `json:"google_cx"`
}

func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	var cfg Config
	err = json.NewDecoder(file).Decode(&cfg)
	return cfg, err
}
