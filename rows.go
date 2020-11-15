package timestreamdriver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

var (
	tsTimeLayout    = "2006-01-02 15:04:05.999999999"
	tsDateLayout    = "2006-01-02"
	typeNameUnknown = timestreamquery.ScalarTypeUnknown
	anyType         = reflect.TypeOf(new(interface{})).Elem()
	intType         = reflect.TypeOf(int(0))
	bigintType      = reflect.TypeOf(int64(0))
	doubleType      = reflect.TypeOf(float64(0))
	boolType        = reflect.TypeOf(true)
	stringType      = reflect.TypeOf("")
	nullType        = reflect.TypeOf(nil)
	timeType        = reflect.TypeOf(time.Time{})
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
	driver.RowsColumnTypeScanType
} = &rows{}

func (r *rows) getColumn(index int) *timestreamquery.ColumnInfo {
	if len(r.rs.columns) <= index {
		return nil
	}
	return r.rs.columns[index]
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	ci := r.getColumn(index)
	if ci == nil {
		return anyType
	}
	switch dt := getTSDataType(ci); dt {
	case timestreamquery.ScalarTypeBigint:
		return bigintType
	case timestreamquery.ScalarTypeBoolean:
		return boolType
	case timestreamquery.ScalarTypeDate:
		return timeType
	case timestreamquery.ScalarTypeDouble:
		return doubleType
	case timestreamquery.ScalarTypeInteger:
		return intType
	case timestreamquery.ScalarTypeIntervalDayToSecond:
		return stringType
	case timestreamquery.ScalarTypeIntervalYearToMonth:
		return stringType
	case timestreamquery.ScalarTypeTime:
		return timeType
	case timestreamquery.ScalarTypeTimestamp:
		return timeType
	case timestreamquery.ScalarTypeVarchar:
		return stringType
	case timestreamquery.ScalarTypeUnknown:
		return nullType
	default:
		return anyType
	}
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return getTSDataType(r.getColumn(index))
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
		parsed, err := parseDate(datum)
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

func parseDate(datum *timestreamquery.Datum) (time.Time, error) {
	parsed, err := time.ParseInLocation(tsDateLayout, *datum.ScalarValue, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func parseTime(datum *timestreamquery.Datum) (time.Time, error) {
	parsed, err := time.ParseInLocation(tsTimeLayout, *datum.ScalarValue, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func getTSDataType(ci *timestreamquery.ColumnInfo) string {
	if ci == nil {
		return typeNameUnknown
	}
	if ci.Type.ScalarType != nil {
		return *ci.Type.ScalarType
	}
	return typeNameUnknown
}
