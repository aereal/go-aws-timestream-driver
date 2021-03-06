package timestreamdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
)

func TestConn_QueryContext_Scalar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(scalarOutput())
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
	testRowsQueryScalar(t, rows)
}

type testLogger struct {
	t  *testing.T
	mu sync.Mutex
}

var _ xraylog.Logger = &testLogger{}

func (l *testLogger) Log(level xraylog.LogLevel, msg fmt.Stringer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t.Logf("[%s][%s] %s", time.Now().Format(time.RFC3339Nano), level, msg)
}

func TestConn_Connector_Xray(t *testing.T) {
	withXrayTest := os.Getenv("WITH_XRAY_TEST")
	t.Logf("WITH_XRAY_TEST=%q", withXrayTest)
	if withXrayTest != "true" {
		t.SkipNow()
	}
	xray.SetLogger(&testLogger{t: t})
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
	ctx, err := xray.ContextWithConfig(context.Background(), xray.Config{SamplingStrategy: alwaysSample(0)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, seg := xray.BeginSegment(ctx, "test")
	defer func() {
		t.Logf("enter defer")
		if seg != nil {
			t.Logf("segment: ContextDone=%v Dummy=%v Emitted=%v InProgress=%v", seg.ContextDone, seg.Dummy, seg.Emitted, seg.InProgress)
			if seg.Emitted {
				seg.Close(nil)
			}
		}
	}()

	rows, err := db.QueryContext(ctx, `SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	t.Logf("[%s] try to close segment", time.Now().Format(time.RFC3339Nano))
	seg.Close(nil)
	t.Logf("[%s] done to close segment", time.Now().Format(time.RFC3339Nano))

	softDeadline := time.Now().Add(time.Second * 30)
	if hardDeadline, ok := deadlineOf(t); ok && softDeadline.After(hardDeadline) {
		softDeadline = hardDeadline
	}
	for {
		s := xray.GetSegment(ctx)
		if s == nil {
			t.Errorf("No segment emitted")
			return
		}
		if s.Emitted {
			break
		}
		if time.Now().After(softDeadline) {
			t.Errorf("No segment emitted after deadline exceeded")
			return
		}
		t.Logf("[%s] wait for segment to be emitted; segment: ContextDone=%v Dummy=%v Emitted=%v InProgress=%v", time.Now().Format(time.RFC3339Nano), seg.ContextDone, seg.Dummy, seg.Emitted, seg.InProgress)
		time.Sleep(time.Second)
	}

	cmpSeg(t, xray.GetSegment(ctx), &xray.Segment{
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

func TestConn_QueryContext_NamedParams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input *timestreamquery.QueryInput
		_ = json.NewDecoder(r.Body).Decode(&input)
		expected := `SELECT age FROM db1.table1 WHERE name = 'yuno'`
		if *input.QueryString != expected {
			t.Errorf("QueryString\n  actual=%q\nexpected=%q", *input.QueryString, expected)
		}
		_ = json.NewEncoder(w).Encode(&timestreamquery.QueryOutput{
			ColumnInfo: []*timestreamquery.ColumnInfo{
				scalarColumn("age", timestreamquery.ScalarTypeInteger),
			},
			Rows: []*timestreamquery.Row{
				{
					Data: []*timestreamquery.Datum{
						{ScalarValue: aws.String("16")},
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
	rows, err := db.QueryContext(ctx, `SELECT age FROM db1.table1 WHERE name = $name$`, sql.Named("name", "yuno"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	expectedColumns := []string{"age"}
	if !reflect.DeepEqual(cols, expectedColumns) {
		t.Errorf("Rows.Columns(): expected=%#v got=%#v", expectedColumns, cols)
	}
	for rows.Next() {
		var (
			c1 int
		)
		if err := rows.Scan(&c1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(c1, 16) {
			t.Errorf("Rows.Scan(): expected=%#v got=%#v", 16, c1)
		}
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
		{"bare parameter", args{"SELECT * FROM db1.table1 WHERE last_login > ago(?)", []driver.NamedValue{{Ordinal: 1, Value: BareStringValue{"7d"}}}}, "SELECT * FROM db1.table1 WHERE last_login > ago(7d)", false},

		{"named/int parameter", args{"SELECT name FROM db1.table1 WHERE age = $age$", []driver.NamedValue{{Name: "age", Ordinal: 1, Value: int64(20)}}}, "SELECT name FROM db1.table1 WHERE age = 20", false},

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
			cmpSeg(t2, v, ev)
		}
	})
}

func marshalSegment(seg *xray.Segment) json.RawMessage {
	ret, _ := json.Marshal(seg)
	return ret
}

type alwaysSample int

var _ sampling.Strategy = alwaysSample(0)

func (alwaysSample) ShouldTrace(r *sampling.Request) *sampling.Decision {
	return &sampling.Decision{Sample: true}
}
