package sqlserver

import (
	"database/sql"
	"strconv"

	"github.com/shu-go/footrest"
)

func init() {
	d := Dialect()
	footrest.RegisterDialect("sqlserver", &d)
}

func Dialect() footrest.Dialect {
	d := footrest.DefaultDialect()
	d.Placeholder = func(num int) string {
		return "@arg" + strconv.Itoa(num)
	}
	d.Arg = func(num int, a any) any {
		return sql.NamedArg{Name: "arg" + strconv.Itoa(num), Value: a}
	}
	return d
}
