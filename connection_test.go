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
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
)

func init() {
	xray.SetLogger(xraylog.NullLogger)
}

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
				scalarColumn("dt", timestreamquery.ScalarTypeDate),
				scalarColumn("ts", timestreamquery.ScalarTypeTimestamp),
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
		{name: "dt", databaseTypeName: timestreamquery.ScalarTypeDate, scanType: reflect.TypeOf(time.Time{})},
		{name: "ts", databaseTypeName: timestreamquery.ScalarTypeTimestamp, scanType: reflect.TypeOf(time.Time{})},
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
		)
		if err := rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11); err != nil {
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
	}
	if !rowsScanned {
		t.Error("No rows scanned")
	}
}

func TestConn_Connector_Xray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&timestreamquery.QueryOutput{
			ColumnInfo: []*timestreamquery.ColumnInfo{},
			Rows:       []*timestreamquery.Row{},
		})
	}))
	defer srv.Close()
	parsedURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	dsn := fmt.Sprintf("awstimestream+%s://%s/?region=us-east-1&accessKeyID=my-id&secretAccessKey=my-secret&enableXray=true", parsedURL.Scheme, parsedURL.Host)
	cn, err := (&Driver{}).OpenConnector(dsn)
	if err != nil {
		t.Fatal(err)
		return
	}
	db := sql.OpenDB(cn)
	ctx, seg := xray.BeginSegment(context.Background(), "test")
	defer func() {
		if seg != nil {
			seg.Close(nil)
		}
	}()

	rows, err := db.QueryContext(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seg.Close(nil)
	cmpSeg(t, seg, &xray.Segment{
		AWS: map[string]interface{}{"xray": xray.SDK{Version: "1.1.0", Type: "X-Ray for Go", RuleName: ""}},
		Subsegments: []json.RawMessage{
			marshalSegment(&xray.Segment{
				AWS:  map[string]interface{}{"operation": "Query", "region": "us-east-1", "request_id": "", "retries": 0},
				HTTP: &xray.HTTPData{Response: &xray.ResponseData{Status: 200, ContentLength: 60}},
				Subsegments: []json.RawMessage{
					marshalSegment(&xray.Segment{}),
					marshalSegment(&xray.Segment{
						Subsegments: []json.RawMessage{
							marshalSegment(&xray.Segment{
								Subsegments: []json.RawMessage{
									marshalSegment(&xray.Segment{}),
								},
							}),
							marshalSegment(&xray.Segment{}),
							marshalSegment(&xray.Segment{}),
						},
					}),
					marshalSegment(&xray.Segment{}),
				},
			}),
		},
	})
}

func unmarshalSubsegment(seg *xray.Segment) []*xray.Segment {
	segs := make([]*xray.Segment, len(seg.Subsegments))
	for i, s := range seg.Subsegments {
		var v *xray.Segment
		_ = json.Unmarshal(s, &v)
		segs[i] = v
	}
	return segs
}

func TestConn_QueryContext_Array(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&timestreamquery.QueryOutput{
			ColumnInfo: []*timestreamquery.ColumnInfo{
				arrayColumn("strs", timestreamquery.ScalarTypeVarchar),
				arrayColumn("ints", timestreamquery.ScalarTypeInteger),
				arrayColumn("doubles", timestreamquery.ScalarTypeDouble),
				arrayColumn("bools", timestreamquery.ScalarTypeBoolean),
				{
					Name: aws.String("nested"),
					Type: &timestreamquery.Type{
						ArrayColumnInfo: arrayColumn("", timestreamquery.ScalarTypeInteger),
					},
				},
			},
			Rows: []*timestreamquery.Row{
				{
					Data: []*timestreamquery.Datum{
						arrayValue("abc", "def"),
						arrayValue("1", "2"),
						arrayValue("1.0", "2.0"),
						arrayValue("true", "false"),
						{ArrayValue: []*timestreamquery.Datum{
							arrayValue("1", "2"),
							arrayValue("3", "4"),
						}},
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
	expectedColumns := []string{"strs", "ints", "doubles", "bools", "nested"}
	if !reflect.DeepEqual(cols, expectedColumns) {
		t.Errorf("Rows.Columns(): expected=%#v got=%#v", expectedColumns, cols)
	}
	for rows.Next() {
		var (
			c1 []string
			c2 []int
			c3 []float64
			c4 []bool
			c5 AnyArray
		)
		if err := rows.Scan(Array(&c1), Array(&c2), Array(&c3), Array(&c4), &c5); err != nil {
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
		expectedNested := []interface{}{[]interface{}{1, 2}, []interface{}{3, 4}}
		vt1 := reflect.ValueOf(expectedNested)
		vt2 := reflect.ValueOf(c5.E)
		if vt1.Type() != vt2.Type() {
			t.Errorf("c5\n  actual.type=%#v\nexpected.type=%#v", vt2, vt1)
		}
		if !reflect.DeepEqual(c5.E, expectedNested) {
			t.Errorf("Rows.Scan():\n  actual=%#v (%d)\nexpected=%#v (%d)", c5.E, len(c5.E.([]interface{})), expectedNested, len(expectedNested))
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
	ci := &timestreamquery.ColumnInfo{
		Type: &timestreamquery.Type{
			ArrayColumnInfo: &timestreamquery.ColumnInfo{
				Type: &timestreamquery.Type{
					ScalarType: &typ}}}}
	if name != "" {
		ci.Name = &name
	}
	return ci
}

func arrayValue(values ...string) *timestreamquery.Datum {
	dm := &timestreamquery.Datum{ArrayValue: []*timestreamquery.Datum{}}
	for _, v := range values {
		vv := v
		dm.ArrayValue = append(dm.ArrayValue, &timestreamquery.Datum{ScalarValue: &vv})
	}
	return dm
}

func cmpHTTP(t *testing.T, actual, expected *xray.HTTPData) bool {
	if (actual == nil) != (expected == nil) {
		t.Errorf("HTTP\n  actual=%#v\nexpected=%#v", actual, expected)
		return false
	}
	if !reflect.DeepEqual(actual.GetRequest(), expected.GetRequest()) {
		t.Errorf("HTTP.Request\n  actual=%#v\nexpected=%#v", actual.Request, expected.Request)
	}
	if !reflect.DeepEqual(actual.GetResponse(), expected.GetResponse()) {
		t.Errorf("HTTP.Response\n  actual=%#v\nexpected=%#v", actual.Response, expected.Response)
	}
	return true
}

func cmpSeg(t *testing.T, actual, expected *xray.Segment) {
	t.Run(fmt.Sprintf("segment:%s", actual.Name), func(t2 *testing.T) {
		if !reflect.DeepEqual(actual.AWS, expected.AWS) {
			t2.Errorf("Seg.AWS:\n  actual=%#v\nexpected=%#v", actual.AWS, expected.AWS)
		}
		cmpHTTP(t2, actual.GetHTTP(), expected.GetHTTP())
		actualSubsegmentsLen := len(actual.Subsegments)
		expectedSubsegmentsLen := len(expected.Subsegments)
		if actualSubsegmentsLen != expectedSubsegmentsLen {
			t2.Errorf("Subsegments.len()\n  actual=%d\nexpected=%d", actualSubsegmentsLen, expectedSubsegmentsLen)
			return
		}
		avs := unmarshalSubsegment(actual)
		evs := unmarshalSubsegment(expected)
		for i, av := range avs {
			v := av
			ev := evs[i]
			// t2.Logf("id=%q name=%q aws=%#v http=%#v", v.ID, v.Name, v.AWS, v.HTTP)
			cmpSeg(t2, v, ev)
		}
	})
}

func marshalSegment(seg *xray.Segment) json.RawMessage {
	ret, _ := json.Marshal(seg)
	return ret
}
