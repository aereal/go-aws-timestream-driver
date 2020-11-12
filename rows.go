package timestreamdriver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

var (
	tsTimeLayout = "2006-01-02 15:04:05.999999999"
)

type resultSet struct {
	columns []*timestreamquery.ColumnInfo
}

type rows struct {
	rs          resultSet
	columnNames []string
	rows        []*timestreamquery.Row
	pos         int
}

var _ interface {
	driver.RowsColumnTypeDatabaseTypeName
} = &rows{}

func (r *rows) getColumn(index int) *timestreamquery.ColumnInfo {
	if len(r.rs.columns) <= index {
		return nil
	}
	return r.rs.columns[index]
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	ci := r.getColumn(index)
	if ci.Type.ScalarType != nil {
		return *ci.Type.ScalarType
	}
	return "UNKNOWN"
}

func (r *rows) Columns() []string {
	if r.columnNames != nil {
		return r.columnNames
	}
	r.columnNames = make([]string, len(r.rs.columns))
	for i, col := range r.rs.columns {
		r.columnNames[i] = *col.Name
	}
	return r.columnNames
}

func (rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.pos == len(r.rows) {
		return io.EOF
	}
	for i, datum := range r.rows[r.pos].Data {
		columnInfo := r.getColumn(i)
		var err error
		dest[i], err = scanColumn(datum, columnInfo)
		if err != nil {
			return err
		}
	}
	r.pos++
	return nil
}

func scanColumn(datum *timestreamquery.Datum, columnInfo *timestreamquery.ColumnInfo) (driver.Value, error) {
	if columnInfo.Type.ArrayColumnInfo != nil {
		return scanArrayColumn(datum, columnInfo)
	}
	if columnInfo.Type.ScalarType != nil {
		return scanScalarColumn(datum, columnInfo)
	}
	return nil, fmt.Errorf("column (%s) not handled", *columnInfo.Name)
}

type columnDatum struct {
	Datum      *timestreamquery.Datum
	ColumnInfo *timestreamquery.ColumnInfo
}

func scanArrayColumn(datum *timestreamquery.Datum, columnInfo *timestreamquery.ColumnInfo) (driver.Value, error) {
	return json.Marshal(columnDatum{datum, columnInfo})
}

func scanScalarColumn(datum *timestreamquery.Datum, columnInfo *timestreamquery.ColumnInfo) (driver.Value, error) {
	switch t := *columnInfo.Type.ScalarType; t {
	case timestreamquery.ScalarTypeBigint:
		i, ok := new(big.Int).SetString(*datum.ScalarValue, 10)
		if !ok {
			return nil, errors.New("out of range")
		}
		if i.IsInt64() {
			return i.Int64(), nil
		}
		if i.IsUint64() {
			return i.Uint64(), nil
		}
		return nil, errors.New("out of range")
	case timestreamquery.ScalarTypeInteger:
		parsed, err := strconv.ParseInt(*datum.ScalarValue, 10, 64)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case timestreamquery.ScalarTypeVarchar:
		return *datum.ScalarValue, nil
	case timestreamquery.ScalarTypeBoolean:
		return *datum.ScalarValue == "true", nil
	case timestreamquery.ScalarTypeDouble:
		parsed, err := strconv.ParseFloat(*datum.ScalarValue, 64)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case timestreamquery.ScalarTypeDate:
		parsed, err := parseTime(datum)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case timestreamquery.ScalarTypeTimestamp:
		parsed, err := parseTime(datum)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case timestreamquery.ScalarTypeTime:
		parsed, err := parseTime(datum)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case timestreamquery.ScalarTypeIntervalDayToSecond:
		return *datum.ScalarValue, nil
	case timestreamquery.ScalarTypeIntervalYearToMonth:
		return *datum.ScalarValue, nil
	case timestreamquery.ScalarTypeUnknown:
		return nil, nil
	default:
		return nil, fmt.Errorf("timestream: cannot convert %s", t)
	}
}

func parseTime(datum *timestreamquery.Datum) (time.Time, error) {
	parsed, err := time.ParseInLocation(tsTimeLayout, *datum.ScalarValue, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}
