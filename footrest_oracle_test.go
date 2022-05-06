package footrest_test

import (
	"testing"

	"github.com/shu-go/gotwant"

	"github.com/shu-go/footrest"

	_ "github.com/shu-go/footrest/dialect/oracle"
)

func TestOracleDialect(t *testing.T) {
	r := footrest.New(nil, "oracle", nil, false, nil)
	//rog.EnableDebug()
	w, args, err := r.BuildGetStmt("users", nil, `(OR (IS .FLAG null) (AND (= .NAME 'HOGE') (NOT (>= .AGE #18)) (<> .ID #0)))`, nil)
	//rog.DisableDebug()
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE (FLAG IS :0) OR ((NAME = :1) AND (NOT (AGE >= :2)) AND (ID <> :3))`)
	gotwant.Test(t, args, []interface{}{nil, "HOGE", 18, 0})

	w, args, err = r.BuildGetStmt("users", nil, `(between .AGE #0 #17)`, nil)
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE AGE BETWEEN :0 AND :1`)
	gotwant.Test(t, args, []interface{}{0, 17})

	w, args, err = r.BuildGetStmt("users", nil, `(like .name (|| 'Mr.' '%'))`, nil)
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, w, `SELECT * FROM users WHERE name LIKE :0 || :1`)
	gotwant.Test(t, args, []interface{}{"Mr.", "%"})
}
