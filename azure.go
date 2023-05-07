package main

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)
import "github.com/AzureAD/microsoft-authentication-library-for-go/apps/confidential"

type AzureFetcher struct {
	Authority string `toml:"authority"`
	ClientId  string `toml:"client-id"`
	CertPath  string `toml:"cert-path"`
	KeyPath   string `toml:"key-path"`
	Scope     string `toml:"scope"`
	Principal string `toml:"principal"`
}

func (f *AzureFetcher) Fetch() (*kafka.OAuthBearerToken, error) {
	result, err := fetchToken(
		f.Authority,
		f.ClientId,
		f.CertPath,
		f.KeyPath,
		f.Scope,
	)
	if err != nil {
		return nil, err
	}

	return &kafka.OAuthBearerToken{
		TokenValue: result.AccessToken,
		Expiration: result.ExpiresOn,
		Principal:  f.Principal,
	}, nil
}

func NewAzureTokenFetcherFromConfig(azureConfigPath string) (*AzureFetcher, error) {
	var fetcher AzureFetcher
	_, err := toml.DecodeFile(azureConfigPath, &fetcher)
	if err != nil {
		return nil, err
	}
	return &fetcher, nil
}

func fetchToken(authority, clientId, certPath, keyPath, scope string) (*confidential.AuthResult, error) {
	certs, err := getCerts(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cert from '%s': %w", certPath, err)
	}

	privateKey, err := getPrivateKey(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key from '%s': %w", keyPath, err)
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
