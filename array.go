package timestreamdriver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type CustomType interface {
	sql.Scanner
	// TODO: driver.Valuer
}

func Array(x interface{}) CustomType {
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

type StringArray []string

var _ CustomType = &StringArray{}

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

type IntegerArray []int

var _ CustomType = &IntegerArray{}

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

type FloatArray []float64

var _ CustomType = &FloatArray{}

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

type BooleanArray []bool

var _ CustomType = &BooleanArray{}

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
