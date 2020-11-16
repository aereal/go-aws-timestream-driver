// +build !go1.15

package timestreamdriver

import (
	"testing"
	"time"
)

func deadlineOf(t *testing.T) (time.Time, bool) {
	t.Helper()
	return time.Time{}, false
}
