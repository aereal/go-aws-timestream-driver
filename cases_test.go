package timestreamdriver

import (
	"database/sql"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

func testRowsQueryScalar(t *testing.T, rows *sql.Rows) {
	defer rows.Close()
	expectedColumns := columnTypeExpectations{
		{name: "int", databaseTypeName: timestreamquery.ScalarTypeInteger, scanType: reflect.TypeOf(int(0))},
		{name: "big", databaseTypeName: timestreamquery.ScalarTypeBigint, scanType: reflect.TypeOf(int64(0))},
		{name: "percent", databaseTypeName: timestreamquery.ScalarTypeDouble, scanType: reflect.TypeOf(float64(0))},
		{name: "bool", databaseTypeName: timestreamquery.ScalarTypeBoolean, scanType: reflect.TypeOf(true)},
		{name: "str", databaseTypeName: timestreamquery.ScalarTypeVarchar, scanType: reflect.TypeOf("")},
		{name: "dur1", databaseTypeName: timestreamquery.ScalarTypeIntervalDayToSecond, scanType: reflect.TypeOf("")},
		{name: "dur2", databaseTypeName: timestreamquery.ScalarTypeIntervalYearToMonth, scanType: reflect.TypeOf("")},
		{name: "nullish", databaseTypeName: timestreamquery.ScalarTypeUnknown, scanType: reflect.TypeOf(nil)},
		{name: "time", databaseTypeName: timestreamquery.ScalarTypeTime, scanType: reflect.TypeOf(time.Time{})},
		{name: "dt", databaseTypeName: timestreamquery.ScalarTypeDate, scanType: reflect.TypeOf(time.Time{})},
		{name: "ts", databaseTypeName: timestreamquery.ScalarTypeTimestamp, scanType: reflect.TypeOf(time.Time{})},
		{name: "nullableInt", databaseTypeName: timestreamquery.ScalarTypeInteger, scanType: reflect.TypeOf(int(0))},
	}
	if cts, err := rows.ColumnTypes(); err == nil {
		expectedColumns.compare(t, cts)
	} else {
		t.Error(err)
	}
	rowsScanned := false
	for rows.Next() {
		rowsScanned = true
		var (
			c1  int
			c2  uint64
			c3  float64
			c4  bool
			c5  string
			c6  string
			c7  string
			c8  interface{}
			c9  time.Time
			c10 time.Time
			c11 time.Time
			c12 *int
		)
		if err := rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11, &c12); err != nil {
			t.Fatal(err)
		}
		if c1 != 1 {
			t.Errorf("c1: expected=%v got=%v", 1, c1)
		}
		if c2 != math.MaxUint64 {
			t.Errorf("c2: expected=%v got=%v", uint64(math.MaxUint64), c2)
		}
		if c3 != 0.5 {
			t.Errorf("c3: expected=%v got=%v", 0.5, c3)
		}
		if c4 != true {
			t.Errorf("c4: expected=%v got=%v", true, c4)
		}
		if c5 != "hi" {
			t.Errorf("c5: expected=%v got=%v", "hi", c5)
		}
		if c6 != "0 01:00:00.000000000" {
			t.Errorf("c6: expected=%v got=%v", "0 01:00:00.000000000", c6)
		}
		if c7 != "90 01:00:00.000000000" {
			t.Errorf("c7: expected=%v got=%v", "90 01:00:00.000000000", c7)
		}
		expectedTime := time.Unix(1262349296, 0).UTC()
		if !expectedTime.Equal(c9) {
			t.Errorf("c9: expected=%s got=%s", expectedTime, c9)
		}
		expectedDate := time.Unix(1262304000, 0).UTC()
		if !expectedDate.Equal(c10) {
			t.Errorf("c10: expected=%s got=%s", expectedDate, c10)
		}
		if !expectedTime.Equal(c11) {
			t.Errorf("c11: expected=%s got=%s", expectedDate, c11)
		}
		if c12 != nil {
			t.Errorf("c12: expected=nil got=%#v", c12)
		}
	}
	if !rowsScanned {
		t.Error("No rows scanned")
	}
}

func scalarOutput() *timestreamquery.QueryOutput {
	return &timestreamquery.QueryOutput{
		ColumnInfo: []*timestreamquery.ColumnInfo{
			scalarColumn("int", timestreamquery.ScalarTypeInteger),
			scalarColumn("big", timestreamquery.ScalarTypeBigint),
			scalarColumn("percent", timestreamquery.ScalarTypeDouble),
			scalarColumn("bool", timestreamquery.ScalarTypeBoolean),
			scalarColumn("str", timestreamquery.ScalarTypeVarchar),
			scalarColumn("dur1", timestreamquery.ScalarTypeIntervalDayToSecond),
			scalarColumn("dur2", timestreamquery.ScalarTypeIntervalYearToMonth),
			scalarColumn("nullish", timestreamquery.ScalarTypeUnknown),
			scalarColumn("time", timestreamquery.ScalarTypeTime),
			scalarColumn("dt", timestreamquery.ScalarTypeDate),
			scalarColumn("ts", timestreamquery.ScalarTypeTimestamp),
			scalarColumn("nullableInt", timestreamquery.ScalarTypeInteger),
		},
		Rows: []*timestreamquery.Row{{
			Data: []*timestreamquery.Datum{
				{ScalarValue: aws.String("1")},
				{ScalarValue: aws.String(strconv.FormatUint(math.MaxUint64, 10))},
				{ScalarValue: aws.String("0.5")},
				{ScalarValue: aws.String("true")},
				{ScalarValue: aws.String("hi")},
				{ScalarValue: aws.String("0 01:00:00.000000000")},
				{ScalarValue: aws.String("90 01:00:00.000000000")},
				{},
				{ScalarValue: aws.String("2010-01-01 12:34:56.000000000")},
				{ScalarValue: aws.String("2010-01-01")},
				{ScalarValue: aws.String("2010-01-01 12:34:56.000000000")},
				{NullValue: aws.Bool(true)},
			},
		}},
	}
}
