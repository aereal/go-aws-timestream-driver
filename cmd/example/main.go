package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	timestreamdriver "github.com/aereal/go-aws-timestream-driver"
	"github.com/jmoiron/sqlx"
)

var (
	measureCPUUtilization      = "cpu_utilization"
	measureMemoryUtilization   = "memory_utilization"
	measureNetworkIngressBytes = "network_ingress_bytes"
	measureNetworkEgressBytes  = "network_egress_bytes"
)

func main() {
	h := &handler{}
	if err := h.run(os.Args[1:]); err != nil {
		log.Printf("! %+v", err)
		os.Exit(1)
	}
}

func (h *handler) run(argv []string) error {
	if err := h.parseFlags(argv); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	db, err := sqlx.Open("awstimestream", "awstimestream:///?region=us-east-1")
	if err != nil {
		return err
	}
	defer db.Close()

	if false {
		if err := h.averageCPUUtilization(ctx, db); err != nil {
			return err
		}
	}
	if err := h.values(ctx, db); err != nil {
		return err
	}
	return nil
}

func (h *handler) averageCPUUtilization(ctx context.Context, db *sqlx.DB) error {
	log.Printf("---> averageCPUUtilization")
	rows, err := db.QueryContext(ctx, `SELECT 1 AS num, role, role = 'db' AS isDB, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
FROM TestDB1.TestTable1
WHERE measure_name = 'cpu_utilization'
    AND time > ago(24h)
GROUP BY role, BIN(time, 30s)
ORDER BY binned_timestamp ASC`)
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	log.Printf("columns=%#v", cols)
	for rows.Next() {
		var (
			num      int
			role     string
			isDB     bool
			binnedTS time.Time
			avgCPU   float64
		)
		if err := rows.Scan(&num, &role, &isDB, &binnedTS, &avgCPU); err != nil {
			return err
		}
		log.Printf("num=%d role=%s isDB=%v ts=%s avg cpu=%.2f", num, role, isDB, binnedTS, avgCPU)
	}
	return nil
}

func (h *handler) showTables(ctx context.Context, db *sqlx.DB) error {
	return nil
}

type ret struct {
	Num    int                          `db:"num"`
	Str    string                       `db:"str"`
	Dur    string                       `db:"dur"`
	Dur2   string                       `db:"dur2"`
	Strs   timestreamdriver.StringArray `db:"parts"`
	Today  time.Time                    `db:"today"`
	TS     time.Time                    `db:"ts"`
	Nested timestreamdriver.AnyArray    `db:"nested"`
}

func (h *handler) values(ctx context.Context, db *sqlx.DB) error {
	var results []ret
	query := `SELECT 1 AS num, 'hello' as str, 1h as dur, 60d as dur2, split('abc/def', '/') AS parts, cast(now() as date) as today, cast(now() as timestamp) as ts, repeat(repeat('A', 3), 3) as nested`
	if err := db.SelectContext(ctx, &results, query); err != nil {
		return err
	}
	for i, r := range results {
		log.Printf("#%d %#v", i, r)
	}
	return nil
}

func (h *handler) parseFlags(argv []string) error {
	fset := flag.NewFlagSet("importdata", flag.ContinueOnError)
	fset.StringVar(&h.dbName, "db", "", "timestream database name")
	fset.StringVar(&h.tableName, "table", "", "timestream table name")
	if err := fset.Parse(argv); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if h.dbName == "" {
		return errors.New("dbName is required")
	}
	if h.tableName == "" {
		return errors.New("tableName is required")
	}
	return nil
}

type handler struct {
	dbName    string
	tableName string
}

func newRandomizer() *rand.Rand {
	now := time.Now()
	return rand.New(rand.NewSource(now.Unix() * now.UnixNano()))
}
