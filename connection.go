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
	// ErrTooFewParameters is an error indicates number of passed parameters less than query's placeholders.
	// It may be returned by Rows.QueryContext().
	ErrTooFewParameters = errors.New("too few parameters passed")

	placeholder = '?'
)

type conn struct {
	tsq timestreamqueryiface.TimestreamQueryAPI
}

var _ interface {
	driver.Conn
	driver.QueryerContext
} = &conn{}

func (conn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{query: query, cn: c}, nil
}

func (conn) Close() error {
	return nil
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
		rows.rs.columns = append(rows.rs.columns, out.ColumnInfo...)
		rows.rows = append(rows.rows, out.Rows...)
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
			shouldQuote := true
			if _, ok := val.(bareValue); ok {
				shouldQuote = false
			}
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
				b.WriteByte('\'')
				b.Write(val)
				b.WriteByte('\'')
			case string:
				if shouldQuote {
					b.WriteByte('\'')
					b.WriteString(val)
					b.WriteByte('\'')
				} else {
					b.WriteString(val)
				}
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
