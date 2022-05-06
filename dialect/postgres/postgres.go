package postgres

import (
	"strconv"

	"github.com/shu-go/footrest"
)

func init() {
	d := Dialect()
	footrest.RegisterDialect("postgres", &d)
}

func Dialect() footrest.Dialect {
	d := footrest.DefaultDialect()
	d.Placeholder = func(num int) string {
		return "$" + strconv.Itoa(num+1)
	}
	return d
}
