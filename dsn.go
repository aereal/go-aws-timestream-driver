package timestreamdriver

import (
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
)

var (
	keyRegion = "region"
	keyKeyID  = "accessKeyID"
	keySecret = "secretAccessKey"

	ErrMissingRegion = errors.New("region parameter required")
)

type Config struct {
	EndpointHostname   string
	Region             string
	CredentialProvider credentials.Provider
}

func parseDSN(dsn string) (*Config, error) {
	df := defaults.Get()
	providers := defaults.CredProviders(df.Config, df.Handlers)

	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	qs := parsed.Query()
	cfg := &Config{CredentialProvider: &credentials.ChainProvider{Providers: providers}}
	if region := qs.Get(keyRegion); region != "" {
		cfg.Region = region
	}
	if endpointHost := parsed.Hostname(); endpointHost != "" {
		cfg.EndpointHostname = endpointHost
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
