package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/sync/errgroup"
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
		os.Exit(0)
	}
}

func (h *handler) run(argv []string) error {
	if err := h.parseFlags(argv); err != nil {
		return err
	}
	roles := []string{"app", "proxy", "db"}
	regions := []string{"us-east-1"}
	azSuffixes := []string{"a", "b", "c"}
	measures := []string{measureCPUUtilization, measureMemoryUtilization, measureNetworkEgressBytes, measureNetworkIngressBytes}
	randomizer := newRandomizer()
	now := time.Now()
	records := []*timestreamwrite.Record{}
	for _, role := range roles {
		hostNums := randomizer.Perm(len(role))
		for _, hostNum := range hostNums {
			hostName := fmt.Sprintf("%s-%02d", role, hostNum+1)
			for _, region := range regions {
				for _, azSuffix := range azSuffixes {
					az := region + azSuffix
					for _, measure := range measures {
						epsilon := time.Second * time.Duration(randomizer.Intn(300)*-1)
						ts := now.Add(epsilon)
						log.Printf("az=%s host=%s epsilon=%v measure=%s", az, hostName, epsilon, measure)
						var (
							value     string
							valueType string
						)
						switch measure {
						case measureCPUUtilization:
							valueType = timestreamwrite.MeasureValueTypeDouble
							value = strconv.FormatFloat(float64(randomizer.Intn(100)/100*100), 'f', 3, 64)
						case measureMemoryUtilization:
							valueType = timestreamwrite.MeasureValueTypeDouble
							value = strconv.FormatFloat(float64(randomizer.Intn(100)/100*100), 'f', 3, 64)
						case measureNetworkEgressBytes:
							valueType = timestreamwrite.MeasureValueTypeBigint
							value = strconv.FormatInt(randomizer.Int63(), 10)
						case measureNetworkIngressBytes:
							valueType = timestreamwrite.MeasureValueTypeBigint
							value = strconv.FormatInt(randomizer.Int63(), 10)
						}
						records = append(records, &timestreamwrite.Record{
							Dimensions: []*timestreamwrite.Dimension{
								{Name: aws.String("region"), Value: &region},
								{Name: aws.String("az"), Value: &az},
								{Name: aws.String("role"), Value: &role},
								{Name: aws.String("host"), Value: &hostName},
							},
							MeasureName:      aws.String(measure),
							MeasureValue:     &value,
							MeasureValueType: &valueType,
							Time:             aws.String(strconv.FormatInt(ts.UnixNano(), 10)),
							TimeUnit:         aws.String(timestreamwrite.TimeUnitNanoseconds),
						})
					}
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	ses := session.Must(session.NewSession())
	ses.Config.Region = aws.String("us-east-1")
	writer := timestreamwrite.New(ses)
	if err := h.flushRecords(ctx, writer, records); err != nil {
		return err
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

var (
	maxRecordsPerReq = 100
)

func (h *handler) flushRecords(ctx context.Context, svc *timestreamwrite.TimestreamWrite, records []*timestreamwrite.Record) error {
	eg, ctx := errgroup.WithContext(ctx)
	chunk := []*timestreamwrite.Record{}
	flush := func() {
		rs := chunk[:]
		log.Printf("flush %d records", len(rs))
		eg.Go(func() error {
			input := &timestreamwrite.WriteRecordsInput{
				DatabaseName: &h.dbName,
				TableName:    &h.tableName,
				Records:      rs,
			}
			_, err := svc.WriteRecordsWithContext(ctx, input)
			if err != nil {
				return err
			}
			return nil
		})
		chunk = []*timestreamwrite.Record{}
	}
	for _, r := range records {
		chunk = append(chunk, r)
		if len(chunk) >= maxRecordsPerReq {
			flush()
		}
	}
	if len(chunk) > 0 {
		flush()
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
