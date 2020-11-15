package timestreamdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/aereal/go-aws-timestream-driver/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

var (
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
	cfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	awsCfg := aws.Config{Credentials: credentials.NewCredentials(cfg.CredentialProvider)}
	if cfg.Region != "" {
		awsCfg.Region = &cfg.Region
	}
	if cfg.EndpointHostname != "" {
		awsCfg.Endpoint = aws.String(fmt.Sprintf("https://%s", cfg.EndpointHostname))
	}
	ses, err := session.NewSessionWithOptions(session.Options{Config: awsCfg})
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
