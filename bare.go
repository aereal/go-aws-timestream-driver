package timestreamdriver

import "database/sql/driver"

type bareValue interface {
	IsBareValue()
}

// BareStringValue is a string parameter but not quoted.
// You can wrap interval literal with this type and then embed interval literal into query.
type BareStringValue struct {
	Bare string
}

var _ interface {
	driver.Valuer
	bareValue
} = BareStringValue{}

func (bsv BareStringValue) Value() (driver.Value, error) {
	return driver.Value(bsv.Bare), nil
}

func (BareStringValue) IsBareValue() {}
