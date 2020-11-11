package timestreamdriver

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
)

var (
	ErrPrepareNotSupported = errors.New("Prepare() not supported")
	ErrBeginNotSupported   = errors.New("Begin() not supported")
	ErrTooFewParameters    = errors.New("too few parameters passed")

	placeholder = '?'
)

type conn struct {
	tsq timestreamqueryiface.TimestreamQueryAPI
}

var _ interface {
	driver.Conn
	driver.QueryerContext
	driver.Queryer
} = &conn{}

func (conn) Begin() (driver.Tx, error) {
	return nil, ErrBeginNotSupported
}

func (conn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrPrepareNotSupported
}

func (conn) Close() error {
	return nil
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	nvs := make([]driver.NamedValue, len(args))
	for i, dv := range args {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: dv}
	}
	return c.queryContext(context.Background(), query, nvs)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	enhancedQuery, err := interpolatesQuery(query, args)
	if err != nil {
		return nil, err
	}
	input := &timestreamquery.QueryInput{QueryString: &enhancedQuery}
	rows := &rows{rs: resultSet{}}
	cb := func(out *timestreamquery.QueryOutput, lastPage bool) bool {
		for _, ci := range out.ColumnInfo {
			rows.rs.columns = append(rows.rs.columns, ci)
		}
		for _, row := range out.Rows {
			rows.rows = append(rows.rows, row)
		}
		return out.NextToken == nil
	}
	if err := c.tsq.QueryPagesWithContext(ctx, input, cb); err != nil {
		return nil, err
	}
	return rows, nil
}

func interpolatesQuery(query string, args []driver.NamedValue) (string, error) {
	b := new(bytes.Buffer)
	placeholderPos := 0
	for _, v := range query {
		if v == placeholder {
			if len(args) < placeholderPos+1 {
				return "", ErrTooFewParameters
			}
			nv := args[placeholderPos]
			val := nv.Value
			if valuer, ok := val.(driver.Valuer); ok {
				var err error
				val, err = valuer.Value()
				if err != nil {
					return "", err
				}
			}
			switch val := val.(type) {
			case int64:
				b.WriteString(strconv.FormatInt(val, 10))
			case float64:
				b.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			case bool:
				b.WriteString(fmt.Sprintf("%v", val))
			case []byte:
				b.WriteString(fmt.Sprintf("'%s'", val))
			case string:
				b.WriteString(fmt.Sprintf("'%s'", val))
			case time.Time:
				b.WriteString(fmt.Sprintf("'%s'", val.Format(tsTimeLayout)))
			default:
				return "", fmt.Errorf("unknown parameter: %#v (%T)", val, val)
			}
			placeholderPos++
		} else {
			b.WriteRune(v)
		}
	}
	return b.String(), nil
}
