package timestreamdriver

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
)

var (
	keyRegion = "region"
	keyKeyID  = "accessKeyID"
	keySecret = "secretAccessKey"
	keyXray   = "enableXray"
)

type Config struct {
	Endpoint           string
	Region             string
	CredentialProvider credentials.Provider
	EnableXray         bool
}

func ParseDSN(dsn string) (*Config, error) {
	df := defaults.Get()
	providers := defaults.CredProviders(df.Config, df.Handlers)

	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	scheme, err := parseScheme(parsed.Scheme)
	if err != nil {
		return nil, err
	}
	qs := parsed.Query()
	cfg := &Config{CredentialProvider: &credentials.ChainProvider{Providers: providers}, EnableXray: qs.Get(keyXray) == "true"}
	if region := qs.Get(keyRegion); region != "" {
		cfg.Region = region
	}
	if endpointHost := parsed.Host; endpointHost != "" {
		cfg.Endpoint = fmt.Sprintf("%s://%s", scheme, endpointHost)
	}
	accessKeyID, secretAccessKey := qs.Get(keyKeyID), qs.Get(keySecret)
	if accessKeyID != "" && secretAccessKey != "" {
		cfg.CredentialProvider = &credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		}}
	}
	return cfg, nil
}

func parseScheme(scheme string) (string, error) {
	if !strings.Contains(scheme, DriverName) {
		return "", errors.New("invalid DSN scheme")
	}
	if scheme == DriverName {
		return "https", nil
	}
	customScheme := strings.Replace(scheme, DriverName+"+", "", 1)
	return customScheme, nil
}
