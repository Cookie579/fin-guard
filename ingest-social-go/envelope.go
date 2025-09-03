// envelope.go
package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// ---------- Slim types you want downstream ----------

type SlimUser struct {
	ID       int64  `json:"id,omitempty"`
	Username string `json:"username,omitempty"`
}

type SlimMessage struct {
	Body      string   `json:"body,omitempty"`
	CreatedAt string   `json:"created_at,omitempty"` // keep original RFC3339 string
	ID        int64    `json:"id,omitempty"`
	Likes     int      `json:"likes,omitempty"`
	Sentiment string   `json:"sentiment,omitempty"` // "Bullish"/"Bearish"/"Neutral"
	Symbols   []string `json:"symbols,omitempty"`
	User      SlimUser `json:"user,omitempty"`
}

// What you put on the wire to Kinesis/Firehose.
type SocialEnvelope struct {
	ID         string      `json:"id"`                   // string form of message ID
	Ticker     string      `json:"ticker"`               // e.g., "NVDA"
	Source     string      `json:"source"`               // "stocktwits"
	Stage      string      `json:"stage"`                // "RAW" or "EVENT"
	FilingDate string      `json:"filingDate,omitempty"` // YYYY-MM-DD (for Firehose partitioning)
	Payload    SlimMessage `json:"payload"`              // slimmed payload only
}

// ---------- Minimal decoder for the raw Stocktwits message ----------
// Covers only the fields we actually use; tolerant to API variance.

type stSentiment struct {
	Basic string `json:"basic"`
}

type stUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type RawStocktwitsMessage struct {
	Body      string       `json:"body"`
	CreatedAt string       `json:"created_at"`
	ID        int64        `json:"id"`
	Likes     int          `json:"likes"`
	Sentiment *stSentiment `json:"sentiment,omitempty"`
	// Stocktwits can send symbols as strings or objects; accept either.
	Symbols []json.RawMessage `json:"symbols"`
	User    stUser            `json:"user"`
}

// ---------- Public helpers ----------

// MakeStocktwitsEnvelope transforms a raw ST message into the slim envelope.
func MakeStocktwitsEnvelope(raw RawStocktwitsMessage, ticker string) SocialEnvelope {
	fd := deriveFilingDate(raw.CreatedAt) // YYYY-MM-DD or ""

	sentiment := ""
	if raw.Sentiment != nil {
		sentiment = raw.Sentiment.Basic
	}

	slim := SlimMessage{
		Body:      raw.Body,
		CreatedAt: raw.CreatedAt,
		ID:        raw.ID,
		Likes:     raw.Likes,
		Sentiment: sentiment,
		Symbols:   extractSymbols(raw.Symbols),
		User:      SlimUser{ID: raw.User.ID, Username: raw.User.Username},
	}

	return SocialEnvelope{
		ID:         strconv.FormatInt(raw.ID, 10),
		Ticker:     strings.ToUpper(ticker),
		Source:     "stocktwits",
		Stage:      "RAW",
		FilingDate: fd,
		Payload:    slim,
	}
}

// MakeStocktwitsEnvelopeFromJSON lets you pass a single raw ST message as JSON bytes.
func MakeStocktwitsEnvelopeFromJSON(rawJSON []byte, ticker string) (SocialEnvelope, error) {
	var r RawStocktwitsMessage
	if err := json.Unmarshal(rawJSON, &r); err != nil {
		return SocialEnvelope{}, err
	}
	return MakeStocktwitsEnvelope(r, ticker), nil
}

// ---------- Internals ----------

func deriveFilingDate(createdAt string) string {
	s := strings.TrimSpace(createdAt)
	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Format("2006-01-02")
	}
	// Try first 10 chars if looks like YYYY-MM-DD...
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		return s[:10]
	}
	return ""
}

// Accepts symbols as ["NVDA","AAPL"] or [{"symbol":"NVDA"}, {"ticker":"AAPL"}]
func extractSymbols(rawSyms []json.RawMessage) []string {
	out := make([]string, 0, len(rawSyms))
	for _, itm := range rawSyms {
		// Try as plain string
		var s string
		if err := json.Unmarshal(itm, &s); err == nil && s != "" {
			out = append(out, s)
			continue
		}
		// Try object with "symbol" or "ticker"
		var obj map[string]any
		if err := json.Unmarshal(itm, &obj); err == nil {
			if v, ok := obj["symbol"].(string); ok && v != "" {
				out = append(out, v)
				continue
			}
			if v, ok := obj["ticker"].(string); ok && v != "" {
				out = append(out, v)
				continue
			}
		}
	}
	return out
}
