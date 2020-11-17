package timestreamdriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

func TestStatement_Prepare_QueryContext_Scalar(t *testing.T) {
	db, close := prepareTestDB()
	defer close()
	ctx := context.Background()
	st, err := db.PrepareContext(ctx, `SELECT 1 FROM table1 WHERE name = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.QueryContext(ctx, "me")
	if err != nil {
		t.Fatal(err)
	}
	testRowsQueryScalar(t, rows)
}

func prepareTestDB() (*sql.DB, func()) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(scalarOutput())
	}))
	tsq := timestreamquery.New(session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:      aws.String("us-east-1"),
			Endpoint:    aws.String(srv.URL),
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		},
	})))

	return sql.OpenDB(&connector{tsq}), func() { srv.Close() }
}
