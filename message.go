package main

import (
	"encoding/json"
)

type message struct {
	Timestamp int64 `json:"timestamp"`
}

func BytesFromTimestamp(timestamp int64) (*[]byte, error) {
	bytes, err := json.Marshal(message{timestamp})
	if err != nil {
		return nil, err
	}
	return &bytes, nil
}
