package sqlite

import "github.com/shu-go/footrest"

func init() {
	d := Dialect()
	footrest.RegisterDialect("sqlite", &d)
}

func Dialect() footrest.Dialect {
	d := footrest.DefaultDialect()
	return d
}
