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
	return &Connector{}, nil
}

var _ interface {
	driver.Driver
	driver.DriverContext
} = &Driver{}

type Connector struct{}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{}, nil
}

func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

type Conn struct {
	// TODO: Execer
	// TODO: ExecerContext
	// TODO: Pinger
	// TODO: Queryer
	// TODO: QueryerContext
	// TODO: NamedValueChecker
	// TODO: SessionResetter
	// TODO: Validator
}

func (c *Conn) Begin() (driver.Tx, error) {
	return &Tx{}, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c *Conn) Close() error {
	return nil
}

type Tx struct{}

func (t *Tx) Commit() error {
	return nil
}

func (t *Tx) Rollback() error {
	return nil
}
