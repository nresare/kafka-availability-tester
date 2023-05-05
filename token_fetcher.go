package main

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)
import "github.com/AzureAD/microsoft-authentication-library-for-go/apps/confidential"

func main() {
	certs, err := getCerts("local-testing/cert.pem")
	if err != nil {
		panic(err)
	}

	privateKey, err := getPrivateKey("local-testing/key.pem")

	cred, err := confidential.NewCredFromCert(certs, privateKey)

	client, err := confidential.New("https://login.microsoftonline.com/1b5955bf-2426-4ced-8412-9fe81bb8bca4",
		"0aec160a-1457-4fef-a5e1-3276d1ef3f42",
		cred,
	)
	if err != nil {
		panic(err)
	}
	result, err := client.AcquireTokenByCredential(context.Background(), []string{"https://noaresare.onmicrosoft.com/kafka/.default"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Result: %s", result.AccessToken)
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
