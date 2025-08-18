package main

import (
    "fmt"
    "os"
)

// loadConfigBytes reads the configuration file containing API keys from
// several potential locations. It tries /app/config.json first (used in
// Kubernetes), then ./config.json, then ../config.json. The first file
// found is returned. An error is returned if none of the files exist or
// cannot be read.
func loadConfigBytes() ([]byte, error) {
    paths := []string{"/app/config.json", "./config.json", "../config.json"}
    for _, p := range paths {
        if b, err := os.ReadFile(p); err == nil {
            return b, nil
        }
    }
    return nil, fmt.Errorf("config.json not found in /app, ./, or ../")
}