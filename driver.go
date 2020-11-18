package timestreamdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-xray-sdk-go/xray"
)

const (
	DriverName = "awstimestream"
)

func init() {
	sql.Register(DriverName, &Driver{})
}

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	awsCfg := aws.Config{Credentials: credentials.NewCredentials(cfg.CredentialProvider)}
	if cfg.Region != "" {
		awsCfg.Region = &cfg.Region
	}
	if cfg.Endpoint != "" {
		awsCfg.Endpoint = aws.String(cfg.Endpoint)
	}
	ses, err := session.NewSessionWithOptions(session.Options{Config: awsCfg})
	if err != nil {
		return nil, err
	}
	if cfg.EnableXray {
		ses = xray.AWSSession(ses)
	}
	tsq := timestreamquery.New(ses)
	return &connector{tsq}, nil
}

var _ interface {
	driver.Driver
	driver.DriverContext
} = &Driver{}
