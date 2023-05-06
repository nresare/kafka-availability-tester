package main

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)
import "github.com/AzureAD/microsoft-authentication-library-for-go/apps/confidential"

func Xmain() {
	token, err := fetchToken(
		"https://login.microsoftonline.com/1b5955bf-2426-4ced-8412-9fe81bb8bca4",
		"0aec160a-1457-4fef-a5e1-3276d1ef3f42",
		"local-testing/cert.pem",
		"local-testing/key.pem",
		"https://noaresare.onmicrosoft.com/kafka/.default",
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Result: %s", *token)
}

func hardcodedFetch() (*kafka.OAuthBearerToken, error) {
	result, err := fetchToken(
		"https://login.microsoftonline.com/1b5955bf-2426-4ced-8412-9fe81bb8bca4",
		"0aec160a-1457-4fef-a5e1-3276d1ef3f42",
		"local-testing/cert.pem",
		"local-testing/key.pem",
		"https://noaresare.onmicrosoft.com/kafka/.default",
	)
	if err != nil {
		return nil, err
	}
	sub := "d0d3b25c-3018-4d57-8342-44bb2a281aab"
	return &kafka.OAuthBearerToken{TokenValue: result.AccessToken, Expiration: result.ExpiresOn, Principal: sub}, nil
}

func fetchToken(authority, clientId, certPath, keyPath, scope string) (*confidential.AuthResult, error) {
	certs, err := getCerts(certPath)
	if err != nil {
		return nil, err
	}

	privateKey, err := getPrivateKey(keyPath)
	if err != nil {
		return nil, err
	}

	cred, err := confidential.NewCredFromCert(certs, privateKey)
	if err != nil {
		return nil, err
	}

	client, err := confidential.New(authority, clientId, cred)
	if err != nil {
		return nil, err
	}
	result, err := client.AcquireTokenByCredential(context.Background(), []string{scope})
	return &result, nil
}

func getCerts(path string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return []*x509.Certificate{cert}, nil
}

func getPrivateKey(path string) (crypto.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}
