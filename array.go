package timestreamdriver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type customType = interface {
	sql.Scanner
	// TODO: driver.Valuer
}

// Array converts `x` into corresponding concrete scannable types.
func Array(x interface{}) customType {
	switch x := x.(type) {
	case []string:
		return (*StringArray)(&x)
	case *[]string:
		return (*StringArray)(x)
	case []int:
		return (*IntegerArray)(&x)
	case *[]int:
		return (*IntegerArray)(x)
	case *[]float64:
		return (*FloatArray)(x)
	case []float64:
		return (*FloatArray)(&x)
	case *[]bool:
		return (*BooleanArray)(x)
	case []bool:
		return (*BooleanArray)(&x)
	default:
		switch reflect.TypeOf(x).Kind() {
		case reflect.Ptr, reflect.Array, reflect.Slice:
			return &AnyArray{E: x}
		default:
			return nil
		}
	}
}

type AnyArray struct{ E interface{} }

var _ sql.Scanner = &AnyArray{}

func (a *AnyArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		var cd columnDatum
		if err := json.Unmarshal(src, &cd); err != nil {
			return err
		}
		ret, err := scanAny(cd)
		if err != nil {
			return err
		}
		a.E = ret
		return nil
	default:
		return fmt.Errorf("timestream: cannot convert %T", src)
	}
}

func scanAny(cd columnDatum) (interface{}, error) {
	if cd.Datum.ScalarValue != nil {
		return scanScalarColumn(cd.Datum, cd.ColumnInfo)
	}
	ret := make([]interface{}, len(cd.Datum.ArrayValue))
	elem := cd.ColumnInfo.Type.ArrayColumnInfo
	for i, v := range cd.Datum.ArrayValue {
		var err error
		ret[i], err = scanAny(columnDatum{ColumnInfo: elem, Datum: v})
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// StringArray is a wrapper type of []string that scannable by database/sql
type StringArray []string

var _ customType = &StringArray{}

func (a *StringArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		var cd columnDatum
		if err := json.Unmarshal(src, &cd); err != nil {
			return err
		}
		xs := make([]string, len(cd.Datum.ArrayValue))
		for i, v := range cd.Datum.ArrayValue {
			xs[i] = *v.ScalarValue
		}
		*a = StringArray(xs)
		return nil
	default:
		return fmt.Errorf("timestream: cannot convert %T", src)
	}
}

// IntegerArray is a wrapper type of []int that scannable by database/sql
type IntegerArray []int

var _ customType = &IntegerArray{}

func (a *IntegerArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		var cd columnDatum
		if err := json.Unmarshal(src, &cd); err != nil {
			return err
		}
		xs := make([]int, len(cd.Datum.ArrayValue))
		for i, v := range cd.Datum.ArrayValue {
			parsed, err := strconv.ParseInt(*v.ScalarValue, 10, 64)
			if err != nil {
				return err
			}
			xs[i] = int(parsed)
		}
		*a = IntegerArray(xs)
		return nil
	default:
		return fmt.Errorf("timestream: cannot convert %T", src)
	}
}

// FloatArray is a wrapper type of []float64 that scannable by database/sql
type FloatArray []float64

var _ customType = &FloatArray{}

func (a *FloatArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		var cd columnDatum
		if err := json.Unmarshal(src, &cd); err != nil {
			return err
		}
		xs := make([]float64, len(cd.Datum.ArrayValue))
		for i, v := range cd.Datum.ArrayValue {
			parsed, err := strconv.ParseFloat(*v.ScalarValue, 64)
			if err != nil {
				return err
			}
			xs[i] = parsed
		}
		*a = FloatArray(xs)
		return nil
	default:
		return fmt.Errorf("timestream: cannot convert %T", src)
	}
}

// BooleanArray is a wrapper type of []bool that scannable by database/sql
type BooleanArray []bool

var _ customType = &BooleanArray{}

func (a *BooleanArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		var cd columnDatum
		if err := json.Unmarshal(src, &cd); err != nil {
			return err
		}
		xs := make([]bool, len(cd.Datum.ArrayValue))
		for i, v := range cd.Datum.ArrayValue {
			xs[i] = *v.ScalarValue == "true"
		}
		*a = BooleanArray(xs)
		return nil
	default:
		return fmt.Errorf("timestream: cannot convert %T", src)
	}
}
