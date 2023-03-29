package main

import (
	"bytes"
	"testing"
)

func TestMessage(t *testing.T) {
	instant := int64(1680022107943072)
	expected := []byte("{\"ts\":1680022107943072,\"seq\":0}")

	actual, err := BytesFromTimestamp(instant, 0)
	if err != nil {
		t.Fatalf("Could not encode instant into json: %v", err)
	}
	if !bytes.Equal(expected, *actual) {
		t.Errorf("Expected '%v' and '%v' to be equal, but they were not", string(expected), string(*actual))
	}
}
