package footrest_test

import (
	"testing"

	"github.com/shu-go/footrest"

	"github.com/shu-go/gotwant"
)

func TestBuildGetStmt(t *testing.T) {
	r := footrest.New(nil, "", nil, false, nil)
	stmt, args, err := r.BuildGetStmt("my_table", footrest.Columns("a", "b", "c"), "(and (= .d #1) (like .e 'hoge%hoge'))", footrest.Columns("-a", "c"))
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `SELECT a, b, c FROM my_table WHERE (d = ?) AND (e LIKE ?) ORDER BY a DESC, c`)
	gotwant.Test(t, args, []any{1, "hoge%hoge"})

	stmt, args, err = r.BuildGetStmt("my_table", nil, "(and (= .d #1) (like .e 'hoge%hoge'))", footrest.Columns("-a", "c"))
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `SELECT * FROM my_table WHERE (d = ?) AND (e LIKE ?) ORDER BY a DESC, c`)
	gotwant.Test(t, args, []any{1, "hoge%hoge"})
}

func TestBuildPostStmt(t *testing.T) {
	r := footrest.New(nil, "", nil, false, nil)
	stmt, args, err := r.BuildPostStmt("my_table", map[string]any{
		"a": 1,
	})
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `INSERT INTO my_table (a) VALUES (?)`)
	gotwant.Test(t, args, []any{1})

	stmt, args, err = r.BuildPostStmt("my_table", []map[string]any{
		{
			"a": 1,
		},
		{
			"b": 1,
		},
	})
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `INSERT INTO my_table (a, b) VALUES (?, ?), (?, ?)`)
	gotwant.Test(t, args, []any{1, nil, nil, 1})
}

func TestPutStmt(t *testing.T) {
	r := footrest.New(nil, "", nil, false, nil)
	stmt, args, err := r.BuildPutStmt("my_table", map[string]any{
		"b": "text",
		"a": 1,
	}, "(and (= .d #1) (like .e 'hoge%hoge'))")
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `UPDATE my_table SET a = ?, b = ? WHERE (d = ?) AND (e LIKE ?)`, gotwant.Format("%q"))
	gotwant.Test(t, args, []any{1, "text", 1, "hoge%hoge"})
}

func TestDeleteStmt(t *testing.T) {
	r := footrest.New(nil, "", nil, false, nil)
	stmt, args, err := r.BuildDeleteStmt("my_table", "(and (= .d #1) (like .e 'hoge%hoge'))")
	gotwant.TestError(t, err, nil)
	gotwant.Test(t, stmt, `DELETE FROM my_table WHERE (d = ?) AND (e LIKE ?)`)
	gotwant.Test(t, args, []any{1, "hoge%hoge"})
}

func BenchmarkStmt(b *testing.B) {
	r := footrest.New(nil, "", nil, false, nil)

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.BuildGetStmt("my_table", footrest.Columns("a", "b", "c"), "(and (= .d #1) (like .e 'hoge%hoge'))", footrest.Columns("-a", "c"))
		}
	})

	b.Run("Post", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.BuildPostStmt("my_table", map[string]any{
				"a": 1,
			})
		}
	})

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.BuildPutStmt("my_table", map[string]any{
				"a": 1,
				"b": "text",
			}, "(and (= .d #1) (like .e 'hoge%hoge'))")
		}
	})

	b.Run("Delete", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.BuildDeleteStmt("my_table", "(and (= .d #1) (like .e 'hoge%hoge'))")
		}
	})

}
