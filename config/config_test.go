package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
)

var (
	defaultProvider *credentials.ChainProvider
	staticProvider  *credentials.StaticProvider
)

func init() {
	df := defaults.Get()
	defaultProvider = &credentials.ChainProvider{Providers: defaults.CredProviders(df.Config, df.Handlers)}
	staticProvider = &credentials.StaticProvider{
		Value: credentials.Value{
			AccessKeyID:     "my-id",
			SecretAccessKey: "my-secret",
		},
	}
	dsnConfigAggr = dsnConfigPairAggr{
		minimal:           dsnConfigPair{"minimal", "awstimestream:///", &Config{EndpointHostname: "", Region: "", CredentialProvider: defaultProvider}},
		customEndpoint:    dsnConfigPair{"custom endpoint", "awstimestream://my.custom.endpoint.example:8000/?region=us-east-1", &Config{EndpointHostname: "my.custom.endpoint.example:8000", Region: "us-east-1", CredentialProvider: defaultProvider}},
		staticCredentials: dsnConfigPair{"static credentials", "awstimestream:///?region=us-east-1&accessKeyID=my-id&secretAccessKey=my-secret", &Config{EndpointHostname: "", Region: "us-east-1", CredentialProvider: staticProvider}},
	}
}

type dsnConfigPair struct {
	name string
	dsn  string
	cfg  *Config
}

type dsnConfigPairAggr struct {
	minimal           dsnConfigPair
	customEndpoint    dsnConfigPair
	staticCredentials dsnConfigPair
}

var dsnConfigAggr dsnConfigPairAggr

func Test_parseDSN(t *testing.T) {
	cases := []struct {
		dsnConfig dsnConfigPair
		wantErr   bool
	}{
		{dsnConfigAggr.minimal, false},
		{dsnConfigAggr.customEndpoint, false},
		{dsnConfigAggr.staticCredentials, false},
	}
	for _, c := range cases {
		t.Run(c.dsnConfig.name, func(t *testing.T) {
			got, err := ParseDSN(c.dsnConfig.dsn)
			if (err != nil) != c.wantErr {
				t.Errorf("parseDSN() error = %v, wantErr %v", err, c.wantErr)
				return
			}
			if err := eqConfig(got, c.dsnConfig.cfg); err != nil {
				t.Error(err)
			}
		})
	}
}

func eqConfig(actual, expected *Config) error {
	if actual.EndpointHostname != expected.EndpointHostname {
		return fmt.Errorf("Endpoint:\n  actual: %s\nexpected: %s", actual.EndpointHostname, expected.EndpointHostname)
	}
	if actual.Region != expected.Region {
		return fmt.Errorf("Region:\n  actual: %s\nexpected: %s", actual.Region, expected.Region)
	}
	if formatCredProvider(actual.CredentialProvider) != formatCredProvider(expected.CredentialProvider) {
		return fmt.Errorf("CredentialsProvider:\n  actual: %T\nexpected: %T", actual.CredentialProvider, expected.CredentialProvider)
	}
	return nil
}

func formatCredProvider(provider credentials.Provider) string {
	switch p := provider.(type) {
	case *credentials.ChainProvider:
		children := make([]string, len(p.Providers))
		for i, childProvider := range p.Providers {
			children[i] = formatCredProvider(childProvider)
		}
		return fmt.Sprintf("%T(%s)", p, strings.Join(children, ";"))
	case *credentials.SharedCredentialsProvider:
		return fmt.Sprintf("%T(Filename=%s;Profile=%s)", p, p.Filename, p.Profile)
	case *credentials.StaticProvider:
		return fmt.Sprintf("%T(AccessKeyId=%s;SecretAccessKey=%s;SessionToken=%s;ProviderName=%s)", p, p.AccessKeyID, p.SecretAccessKey, p.SessionToken, p.ProviderName)
	default:
		return fmt.Sprintf("%T", p)
	}
}
