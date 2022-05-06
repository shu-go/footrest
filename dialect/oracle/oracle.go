package oracle

import (
	"strconv"

	"github.com/shu-go/footrest"
)

func init() {
	d := Dialect()
	footrest.RegisterDialect("oracle", &d)
}

func Dialect() footrest.Dialect {
	d := footrest.DefaultDialect()
	d.Placeholder = func(num int) string {
		return ":" + strconv.Itoa(num)
	}
	return d
}
