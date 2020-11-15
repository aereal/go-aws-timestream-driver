package timestreamdriver

import (
	"reflect"
	"testing"
)

func TestArray(t *testing.T) {
	cases := []struct {
		name string
		arg  interface{}
		want CustomType
	}{
		{"strings", []string{"a", "b"}, &StringArray{"a", "b"}},
		{"integers", []int{1, 2}, &IntegerArray{1, 2}},
		{"float values", []float64{1.0, 2.0}, &FloatArray{1.0, 2.0}},
		{"booleans", []bool{false, true}, &BooleanArray{false, true}},
		{"nested", [][]string{{"a", "b"}, {"c", "d"}}, &AnyArray{E: [][]string{{"a", "b"}, {"c", "d"}}}},
		{"unhandleable type", "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Array(c.arg); !reflect.DeepEqual(got, c.want) {
				t.Errorf("Array() = %#v, want %#v", got, c.want)
			}
		})
	}
}
