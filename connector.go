package timestreamdriver

import (
	"context"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
)

// NewConnector returns new driver.Connector instance.
func NewConnector(tsq timestreamqueryiface.TimestreamQueryAPI) driver.Connector {
	return &connector{tsq}
}

type connector struct {
	tsq timestreamqueryiface.TimestreamQueryAPI
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &conn{c.tsq}, nil
}

func (connector) Driver() driver.Driver {
	return &Driver{}
}
