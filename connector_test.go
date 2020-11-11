package timestreamdriver

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/timestreamquery/timestreamqueryiface"
)

func TestConnector_Driver(t *testing.T) {
	type fields struct {
		tsq timestreamqueryiface.TimestreamQueryAPI
	}
	cases := []struct {
		name   string
		fields fields
		want   driver.Driver
	}{
		{"ok", fields{}, &Driver{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			connector := NewConnector(c.fields.tsq)
			if got := connector.Driver(); !reflect.DeepEqual(got, c.want) {
				t.Errorf("Connector.Driver() = %v, want %v", got, c.want)
			}
		})
	}
}
