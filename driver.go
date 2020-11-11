package timestreamdriver

import (
	"context"
	"database/sql/driver"
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
	return NewConnector(nil), nil
}

var _ interface {
	driver.Driver
	driver.DriverContext
} = &Driver{}
