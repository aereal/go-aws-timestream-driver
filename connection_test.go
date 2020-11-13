package timestreamdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

func TestConn_QueryContext_Scalar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&timestreamquery.QueryOutput{
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
				},
			}},
		})
	}))
	defer srv.Close()
	tsq := timestreamquery.New(session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String("us-east-1"),
			Endpoint:    aws.String(srv.URL),
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		},
	})))

	ctx := context.Background()
	db := sql.OpenDB(&connector{tsq})
	rows, err := db.QueryContext(ctx, `SELECT 1 AS num`)
	if err != nil {
		t.Fatal(err)
	}
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
			c1 int
			c2 uint64
			c3 float64
			c4 bool
			c5 string
			c6 string
			c7 string
			c8 interface{}
			c9 time.Time
		)
		if err := rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9); err != nil {
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
	}
	if !rowsScanned {
		t.Error("No rows scanned")
	}
}

func TestConn_QueryContext_Array(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&timestreamquery.QueryOutput{
			ColumnInfo: []*timestreamquery.ColumnInfo{
				arrayColumn("strs", timestreamquery.ScalarTypeVarchar),
				arrayColumn("ints", timestreamquery.ScalarTypeInteger),
				arrayColumn("doubles", timestreamquery.ScalarTypeDouble),
				arrayColumn("bools", timestreamquery.ScalarTypeBoolean),
			},
			Rows: []*timestreamquery.Row{
				{
					Data: []*timestreamquery.Datum{
						arrayValue("abc", "def"),
						arrayValue("1", "2"),
						arrayValue("1.0", "2.0"),
						arrayValue("true", "false"),
					},
				},
			},
		})
	}))
	defer srv.Close()
	tsq := timestreamquery.New(session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String("us-east-1"),
			Endpoint:    aws.String(srv.URL),
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		},
	})))

	ctx := context.Background()
	db := sql.OpenDB(&connector{tsq})
	rows, err := db.QueryContext(ctx, `SELECT split('abc/def', '/') AS strs, [1, 2] AS ints, [1.0, 2.0] AS doubles, [true, false] AS bools`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	expectedColumns := []string{"strs", "ints", "doubles", "bools"}
	if !reflect.DeepEqual(cols, expectedColumns) {
		t.Errorf("Rows.Columns(): expected=%#v got=%#v", expectedColumns, cols)
	}
	for rows.Next() {
		var (
			c1 []string
			c2 []int
			c3 []float64
			c4 []bool
		)
		if err := rows.Scan(Array(&c1), Array(&c2), Array(&c3), Array(&c4)); err != nil {
			t.Fatal(err)
		}
		expectedStrs := []string{"abc", "def"}
		if !reflect.DeepEqual(c1, expectedStrs) {
			t.Errorf("Rows.Scan(): expected=%#v got=%#v", expectedStrs, c1)
		}
		expectedInts := []int{1, 2}
		if !reflect.DeepEqual(c2, expectedInts) {
			t.Errorf("Rows.Scan(): expected=%#v got=%#v", expectedInts, c2)
		}
		expectedDoubles := []float64{1, 2}
		if !reflect.DeepEqual(c3, expectedDoubles) {
			t.Errorf("Rows.Scan(): expected=%#v got=%#v", expectedDoubles, c3)
		}
		expectedBools := []bool{true, false}
		if !reflect.DeepEqual(c4, expectedBools) {
			t.Errorf("Rows.Scan(): expected=%#v got=%#v", expectedBools, c4)
		}
	}
}

type yuno struct{}

var _ driver.Valuer = &yuno{}

func (yuno) Value() (driver.Value, error) {
	return "yuno", nil
}

func Test_interpolatesQuery(t *testing.T) {
	type args struct {
		query string
		args  []driver.NamedValue
	}
	cases := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"no placeholders", args{"SELECT 1", []driver.NamedValue{}}, "SELECT 1", false},
		{"int parameter", args{"SELECT name FROM db1.table1 WHERE age = ?", []driver.NamedValue{{Ordinal: 1, Value: int64(20)}}}, "SELECT name FROM db1.table1 WHERE age = 20", false},
		{"string parameter", args{"SELECT age FROM db1.table1 WHERE name = ?", []driver.NamedValue{{Ordinal: 1, Value: "yuno"}}}, "SELECT age FROM db1.table1 WHERE name = 'yuno'", false},
		{"valuer parameter", args{"SELECT age FROM db1.table1 WHERE name = ?", []driver.NamedValue{{Ordinal: 1, Value: &yuno{}}}}, "SELECT age FROM db1.table1 WHERE name = 'yuno'", false},

		{"less parameters", args{"SELECT name FROM db1.table1 WHERE age = ?", []driver.NamedValue{}}, "", true},
		{"unhandleable parameters", args{"SELECT name FROM db1.table1 WHERE age = ?", []driver.NamedValue{{Ordinal: 1, Value: []string{"hi"}}}}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := interpolatesQuery(c.args.query, c.args.args)
			if (err != nil) != c.wantErr {
				t.Errorf("wantErr=%v err=%v", c.wantErr, err)
				return
			}
			if got != c.want {
				t.Errorf("mismatch\nexpected: %q\n     got: %q", c.want, got)
			}
		})
	}
}

type columnTypeExpectation struct {
	name             string
	databaseTypeName string
	scanType         reflect.Type
}

func (e columnTypeExpectation) compare(ct *sql.ColumnType) error {
	if actual := ct.Name(); e.name != actual {
		return fmt.Errorf("Name: actual=%q expected=%q", actual, e.name)
	}
	if actual := ct.DatabaseTypeName(); e.databaseTypeName != actual {
		return fmt.Errorf("DatabaseTypeName: actual=%q expected=%q", actual, e.databaseTypeName)
	}
	if actual := ct.ScanType(); actual != e.scanType {
		return fmt.Errorf("ScanType: actual=%s expected=%s", actual, e.scanType)
	}
	return nil
}

type columnTypeExpectations []columnTypeExpectation

func (expectations columnTypeExpectations) compare(t *testing.T, columnTypes []*sql.ColumnType) bool {
	if len(columnTypes) != len(expectations) {
		t.Errorf("length mismatch: expected %d items; got %d items", len(expectations), len(columnTypes))
		return false
	}

	for i, ce := range expectations {
		actual := columnTypes[i]
		if err := ce.compare(actual); err != nil {
			t.Errorf("#%d: %s", i, err)
		}
	}
	return true
}

func scalarColumn(name, typ string) *timestreamquery.ColumnInfo {
	return &timestreamquery.ColumnInfo{
		Name: &name,
		Type: &timestreamquery.Type{
			ScalarType: &typ,
		},
	}
}

func arrayColumn(name, typ string) *timestreamquery.ColumnInfo {
	return &timestreamquery.ColumnInfo{
		Name: &name,
		Type: &timestreamquery.Type{
			ArrayColumnInfo: &timestreamquery.ColumnInfo{
				Type: &timestreamquery.Type{
					ScalarType: &typ}}}}
}

func arrayValue(values ...string) *timestreamquery.Datum {
	dm := &timestreamquery.Datum{ArrayValue: []*timestreamquery.Datum{}}
	for _, v := range values {
		vv := v
		dm.ArrayValue = append(dm.ArrayValue, &timestreamquery.Datum{ScalarValue: &vv})
	}
	return dm
}