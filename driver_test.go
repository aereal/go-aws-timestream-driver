package timestreamdriver

import (
	"database/sql/driver"
	"testing"
)

func TestDriver_Open(t *testing.T) {
	tests := []struct {
		name    string
		d       *Driver
		dsn     string
		want    driver.Conn
		wantErr bool
	}{
		{"ok", &Driver{}, "", &conn{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.Open(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer got.Close()
		})
	}
}
