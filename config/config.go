package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	GoogleAPIKeys []string `json:"google_api_keys"`
	GoogleAPIKey  string   `json:"google_api_key"`
	SerperAPIKey  string   `json:"serper_api_key"`
	SerperKeys    []string `json:"serper_keys"`

	// Fallback: finguard-esg (esg.finguard@gmail.com) | 1: finguard-esg-2
	GoogleCX      string `json:"google_cx"`
	TwitterBearer string `json:"twitter_bearer"`

	stocktwitkeys []string `json:"stocktwit_keys"`

	// 1: agasthya.mc@gmail.com | 2: cookieh777@gmail.com | 3: torylanezzz333@gmail.com | 4: esg.finguard@gmail.com | 5: warrenharold119@gmail.com | 6: aspfinances727@gmail.com | 7: jeffhuckle797@gmail.com | 8: temp burner | 9: jonesclifford119@gmail.com | 10: ajaygrifter@gmail.com

	RedditClientID     string `json:"reddit_client_id"`
	RedditClientSecret string `json:"reddit_client_secret"`
	RedditUsername     string `json:"reddit_username"`
	RedditPassword     string `json:"reddit_password"`
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	var cfg Config
	err = json.NewDecoder(f).Decode(&cfg)
	return cfg, err
}
