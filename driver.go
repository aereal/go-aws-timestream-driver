package timestreamdriver

import (
	"context"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	ses, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: credentials.NewCredentials(cfg.CredentialProvider),
			Region:      &cfg.Region,
			Endpoint:    &cfg.Endpoint,
		},
	})
	if err != nil {
		return nil, err
	}
	tsq := timestreamquery.New(ses)
	return &connector{tsq}, nil
}

var _ interface {
	driver.Driver
	driver.DriverContext
} = &Driver{}
