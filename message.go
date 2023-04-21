package main

import (
	"encoding/json"
)

type message struct {
	Timestamp int64  `json:"ts"`
	Sequence  uint64 `json:"seq"`
}

func BytesFromTimestamp(timestamp int64, sequence uint64) (*[]byte, error) {
	bytes, err := json.Marshal(message{timestamp, sequence})
	if err != nil {
		return nil, err
	}
	return &bytes, nil
}
