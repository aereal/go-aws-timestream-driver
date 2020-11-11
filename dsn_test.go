package timestreamdriver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
)

func Test_parseDSN(t *testing.T) {
	df := defaults.Get()
	defaultCredProvider := &credentials.ChainProvider{Providers: defaults.CredProviders(df.Config, df.Handlers)}
	customCred := &credentials.StaticProvider{
		Value: credentials.Value{
			AccessKeyID:     "my-id",
			SecretAccessKey: "my-secret",
		},
	}

	cases := []struct {
		name    string
		dsn     string
		want    *Config
		wantErr bool
	}{
		{"minimal", "awstimestream:///?region=us-east-1", &Config{Endpoint: "", Region: "us-east-1", CredentialProvider: defaultCredProvider}, false},
		{"custom endpoint", "awstimestream://my.custom.endpoint.example/?region=us-east-1", &Config{Endpoint: "https://my.custom.endpoint.example", Region: "us-east-1", CredentialProvider: defaultCredProvider}, false},
		{"static credentials", "awstimestream:///?region=us-east-1&accessKeyID=my-id&secretAccessKey=my-secret", &Config{Endpoint: "", Region: "us-east-1", CredentialProvider: customCred}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseDSN(c.dsn)
			if (err != nil) != c.wantErr {
				t.Errorf("parseDSN() error = %v, wantErr %v", err, c.wantErr)
				return
			}
			if err := eqConfig(got, c.want); err != nil {
				t.Error(err)
			}
		})
	}
}

func eqConfig(actual, expected *Config) error {
	if actual.Endpoint != expected.Endpoint {
		return fmt.Errorf("Endpoint:\n  actual: %s\nexpected: %s", actual.Endpoint, expected.Endpoint)
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
