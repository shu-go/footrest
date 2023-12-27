package sqlserver

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/shu-go/footrest/footrest"
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

	d.Paginate = func(rowsPerPage, page uint) [2]string {
		if rowsPerPage == 0 || page == 0 {
			return [2]string{
				"", "",
			}
		}

		offset := rowsPerPage * (page - 1)
		return [2]string{
			"", fmt.Sprintf("OFFSET %d ROWS FETCH FIRST %d ROWS ONLY", offset, rowsPerPage),
		}
	}

	return d
}
