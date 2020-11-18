package timestreamdriver

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
)

var (
	// ErrTooFewParameters is an error indicates number of passed parameters less than query's placeholders.
	// It may be returned by Rows.QueryContext().
	ErrTooFewParameters = errors.New("too few parameters passed")

	placeholder         = '?'
	namedParamDelimiter = "$"
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
	namedParamExpander, err := buildNamedParamExpander(args)
	if err != nil {
		return "", err
	}

	b := new(bytes.Buffer)
	placeholderPos := 0
	for _, v := range query {
		if v == placeholder {
			if len(args) < placeholderPos+1 {
				return "", ErrTooFewParameters
			}
			if err := formatParam(b, args[placeholderPos].Value); err != nil {
				return "", err
			}
			placeholderPos++
		} else {
			b.WriteRune(v)
		}
	}
	return namedParamExpander.Replace(b.String()), nil
}

func formatParam(buf *bytes.Buffer, val driver.Value) error {
	shouldQuote := true
	if _, ok := val.(bareValue); ok {
		shouldQuote = false
	}
	if valuer, ok := val.(driver.Valuer); ok {
		var err error
		val, err = valuer.Value()
		if err != nil {
			return err
		}
	}
	switch val := val.(type) {
	case int64:
		buf.WriteString(strconv.FormatInt(val, 10))
	case float64:
		buf.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
	case bool:
		buf.WriteString(fmt.Sprintf("%v", val))
	case []byte:
		buf.WriteByte('\'')
		buf.Write(val)
		buf.WriteByte('\'')
	case string:
		if shouldQuote {
			buf.WriteByte('\'')
			buf.WriteString(val)
			buf.WriteByte('\'')
		} else {
			buf.WriteString(val)
		}
	case time.Time:
		buf.WriteString(fmt.Sprintf("'%s'", val.Format(tsTimeLayout)))
	default:
		return fmt.Errorf("unknown parameter: %#v (%T)", val, val)
	}
	return nil
}

func buildNamedParamExpander(nvs []driver.NamedValue) (*strings.Replacer, error) {
	args := []string{}
	seen := map[string]bool{}
	for _, nv := range nvs {
		if nv.Name == "" {
			continue
		}
		if seen[nv.Name] {
			return nil, fmt.Errorf("named parameter (%q) appears multiple times", nv.Name)
		}
		seen[nv.Name] = true
		buf := new(bytes.Buffer)
		if err := formatParam(buf, nv.Value); err != nil {
			return nil, fmt.Errorf("cannot format parameter: %w", err)
		}
		args = append(args, namedParamDelimiter+nv.Name+namedParamDelimiter, buf.String())
	}
	return strings.NewReplacer(args...), nil
}
