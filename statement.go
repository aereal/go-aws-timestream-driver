package timestreamdriver

import (
	"context"
	"database/sql/driver"
)

type stmt struct {
	query string
	cn    *conn
}

var _ interface {
	driver.Stmt
	driver.StmtQueryContext
} = &stmt{}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	vs := make([]driver.NamedValue, len(args))
	for i, a := range args {
		vs[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return s.QueryContext(context.Background(), vs)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.cn.QueryContext(ctx, s.query, args)
}
