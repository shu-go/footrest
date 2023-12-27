package oracle

import (
	"fmt"
	"strconv"

	"github.com/shu-go/footrest/footrest"
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
