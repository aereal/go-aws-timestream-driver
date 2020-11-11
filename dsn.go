package timestreamdriver

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
)

var (
	keyRegion = "region"

	ErrMissingRegion = errors.New("region parameter required")
)

type Config struct {
	Endpoint           string
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
	region := qs.Get(keyRegion)
	if region == "" {
		return nil, ErrMissingRegion
	}
	cfg := &Config{
		Region:             region,
		CredentialProvider: &credentials.ChainProvider{Providers: providers},
	}
	if parsed.Hostname() != "" {
		cfg.Endpoint = fmt.Sprintf("https://%s", parsed.Hostname())
	}
	return cfg, nil
}
