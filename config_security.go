package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
)

const certPrefix = "-----BEGIN " // magic string for PEM/Cert/Key (when not file path)

// ErrTLSCannotLoadCACerts is returned when the certs file cannot be loaded
var ErrTLSCannotLoadCACerts = errors.New("cannot load CA Certs")

// SecurityConfig is common to producers and consumer configs, above
type SecurityConfig struct {
	RootCACerts        string
	ClientCert         string
	ClientKey          string
	InsecureSkipVerify bool
}

func GetSecurityConfig(caCerts, clientCert, clientKey string, skipVerify bool) *SecurityConfig {
	return &SecurityConfig{
		RootCACerts:        caCerts,
		ClientCert:         clientCert,
		ClientKey:          clientKey,
		InsecureSkipVerify: skipVerify,
	}
}

func expandNewlines(s string) string {
	return strings.ReplaceAll(s, `\n`, "\n")
}

func addAnyTLS(tlsConfig *SecurityConfig, saramaConfig *sarama.Config) error {
	if tlsConfig == nil {
		return nil
	}

	var saramaTLSConfig *tls.Config
	if strings.HasPrefix(tlsConfig.ClientCert, certPrefix) {
		// create cert from strings (not files), cf https://github.com/Shopify/sarama/blob/master/tools/tls/config.go
		cert, err := tls.X509KeyPair(
			[]byte(expandNewlines(tlsConfig.ClientCert)),
			[]byte(expandNewlines(tlsConfig.ClientKey)))
		if err != nil {
			return fmt.Errorf("error parsing X509 keypair from certificate string: %w", err)
		}
		saramaTLSConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	} else {
		// cert in files
		var err error
		saramaTLSConfig, err = saramatls.NewConfig(tlsConfig.ClientCert, tlsConfig.ClientKey)
		if err != nil {
			return fmt.Errorf("error creating new sarama TLS config from files: %w", err)
		}
	}

	if tlsConfig.RootCACerts != "" {
		var rootCAsBytes []byte
		if strings.HasPrefix(tlsConfig.RootCACerts, certPrefix) {
			rootCAsBytes = []byte(expandNewlines(tlsConfig.RootCACerts))
		} else {
			var err error
			rootCAsBytes, err = ioutil.ReadFile(tlsConfig.RootCACerts)
			if err != nil {
				return fmt.Errorf("failed read from %q: %w", tlsConfig.RootCACerts, err)
			}
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(rootCAsBytes) {
			return fmt.Errorf("failed load from %q: %w", tlsConfig.RootCACerts, ErrTLSCannotLoadCACerts)
		}
		// Use specific root CA set vs the host's set
		saramaTLSConfig.RootCAs = certPool
	}

	if tlsConfig.InsecureSkipVerify {
		saramaTLSConfig.InsecureSkipVerify = true
	}

	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = saramaTLSConfig

	return nil
}
