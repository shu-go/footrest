package footrest_test

import (
	"testing"

	_ "modernc.org/sqlite"

	"github.com/shu-go/gotwant"

	"github.com/shu-go/footrest/footrest"
	_ "github.com/shu-go/footrest/footrest/dialect/sqlite"
)

func TestSQLiteDialect(t *testing.T) {
	r := footrest.New(nil, "sqlite", nil, false, nil)
	//rog.EnableDebug()
	w, args, err := r.BuildGetStmt("users", nil, `(OR (IS .FLAG null) (AND (= .NAME 'HOGE') (NOT (>= .AGE #18)) (<> .ID #0)))`, nil, 0, 0)
	//rog.DisableDebug()
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE (FLAG IS ?) OR ((NAME = ?) AND (NOT (AGE >= ?)) AND (ID <> ?))`)
	gotwant.Test(t, args, []interface{}{nil, "HOGE", 18, 0})

	w, args, err = r.BuildGetStmt("users", nil, `(between .AGE #0 #17)`, nil, 0, 0)
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE AGE BETWEEN ? AND ?`)
	gotwant.Test(t, args, []interface{}{0, 17})

	w, args, err = r.BuildGetStmt("users", nil, `(like .name (|| 'Mr.' '%'))`, nil, 0, 0)
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE name LIKE ? || ?`)
	gotwant.Test(t, args, []interface{}{"Mr.", "%"})
}
