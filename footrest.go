package footrest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/shu-go/rog"
	"github.com/shu-go/stacktrace"

	"github.com/fvbommel/sexpr"

	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/text/encoding"
)

type FootREST struct {
	conn      *sql.DB
	dialect   *Dialect
	encoding  encoding.Encoding
	useSchema bool

	scMut       sync.Mutex
	schemaCache map[string](map[string]*sql.ColumnType)

	colConds []colCond // prefix of a query parameter => where notation

	config Config
}

// Columns is for FootREST.Get(ctx, "my_table", Columns("a", "b", "c"), ...)
func Columns(cols ...string) []string {
	if len(cols) == 0 {
		return nil
	}
	return cols
}

type colCond struct {
	name string
	f    func(k, v string) string
}

// New creates new FootREST with already Opened connection(*sql.DB).
func New(conn *sql.DB, dialect string, enc encoding.Encoding, useSchema bool, config *Config) *FootREST {
	d := GetDialect(dialect)

	r := FootREST{
		conn:      conn,
		dialect:   d,
		encoding:  enc,
		useSchema: useSchema,
	}

	if config == nil {
		r.config = *(DefaultConfig())
	} else {
		r.config = *config
	}

	r.colConds = append(r.colConds, colCond{
		name: ">=",
		f: func(k, v string) string {
			return fmt.Sprintf("(>= .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: ">",
		f: func(k, v string) string {
			return fmt.Sprintf("(> .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: "<=",
		f: func(k, v string) string {
			return fmt.Sprintf("(<= .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: "<",
		f: func(k, v string) string {
			return fmt.Sprintf("(< .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: "%",
		f: func(k, v string) string {
			return fmt.Sprintf("(like .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: "!",
		f: func(k, v string) string {
			return fmt.Sprintf("(!= .%v %v)", k, v)
		},
	})
	r.colConds = append(r.colConds, colCond{
		name: "=",
		f: func(k, v string) string {
			return fmt.Sprintf("(= .%v %v)", k, v)
		},
	})

	return &r
}

// NewConn opens a connection and creates new FootREST with the conn.
func NewConn(driverName, dataSourceName string, enc encoding.Encoding, useSchema bool, config *Config) (*FootREST, *sql.DB, error) {
	conn, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, nil, err
	}

	return New(conn, driverName, enc, useSchema, config), conn, nil
}

func (r *FootREST) Serve() {
	restGet := func() echo.HandlerFunc {
		return func(c echo.Context) error {
			table := strings.ToUpper(c.Param("table"))
			sel := strings.ToUpper(c.QueryParam(r.config.Params.Select))
			where := strings.ToUpper(c.QueryParam(r.config.Params.Where))
			order := strings.ToUpper(c.QueryParam(r.config.Params.Order))

			var extraWhere []string
			for k, v := range c.QueryParams() {
				if equalsToAnyOfUpper(k, r.config.Params.Select, r.config.Params.Where, r.config.Params.Order, r.config.Params.Rows, r.config.Params.Page) {
					continue
				}

				for _, vv := range v {
					var cond func(k, v string) string
					for _, cc := range r.colConds {
						if strings.HasPrefix(strings.ToUpper(vv), strings.ToUpper(cc.name)) {
							cond = cc.f
							vv = vv[len(cc.name):]
						}
					}
					if cond == nil {
						cond = func(k, v string) string {
							return fmt.Sprintf("(= .%v %v)", k, v)
						}
					}
					extraWhere = append(extraWhere, cond(k, vv))
				}
			}
			if len(extraWhere) > 0 {
				where = fmt.Sprintf("(AND %v %v)", where, strings.Join(extraWhere, ""))
			}

			if sel == "" {
				sel = "*"
			}
			selColumns := strings.Split(sel, ",")
			orderColumns := strings.Split(order, ",")
			if order == "" {
				orderColumns = nil
			}

			var rows, page uint
			if test, err := strconv.ParseInt(c.QueryParam(r.config.Params.Rows), 10, 64); err == nil {
				rows = uint(test)
			}
			if test, err := strconv.ParseInt(c.QueryParam(r.config.Params.Page), 10, 64); err == nil {
				page = uint(test)
			}

			ctx, cancel := r.config.Context()
			defer cancel()
			colnames, rs, err := r.Get(ctx, table, selColumns, where, orderColumns, rows, page)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			buf := bytes.Buffer{}
			//buf.WriteString(`{"result": [`)
			buf.WriteString(r.config.Format.QueryOK[:strings.Index(r.config.Format.QueryOK, "%")])
			for i, r := range rs {
				if i > 0 {
					buf.WriteByte(',')
				}

				buf.WriteByte('{')

				for col := range colnames {
					if col > 0 {
						buf.WriteByte(',')
					}

					buf.WriteByte('"')
					buf.WriteString(colnames[col])
					buf.WriteString(`": `)

					if s, ok := r[col].(string); ok {
						buf.WriteString(fmt.Sprintf(`%q`, escape(s)))
					} else if r[col] == nil {
						buf.WriteString("null")
					} else {
						buf.WriteString(fmt.Sprintf(`%v`, r[col]))
					}
				}

				buf.WriteByte('}')
			}
			//buf.WriteString(`]}`)
			buf.WriteString(r.config.Format.QueryOK[strings.Index(r.config.Format.QueryOK, "%")+1:])

			return c.String(http.StatusOK, buf.String())
		}
	}

	restBulk := func() echo.HandlerFunc {
		return func(c echo.Context) error {
			//
			// POST http://localhost:12345/!bulk HTTP/1.1
			// content-type: application/json
			//
			// [
			//   {
			//     "method": "DELETE",
			//     "table": "Table1",
			//     "where": {"Col1": "'123'", "Col2": ">=100"}
			//   },
			//   {
			//     "method": "POST",
			//     "table": "Table1",
			//     "values": {"Col3": "12345", "Col4": 23456}
			//   },
			//   {
			//     "method": "PUT",
			//     "table": "Table1",
			//     "where": {"Col1": "'123'", "Col2": ">=100"},
			//     "values": {"Col3": "23456", "Col4": 34567}
			//   }
			// ]
			//

			data, err := io.ReadAll(c.Request().Body)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			var b bulk
			//b := make(bulk, 0, 10)
			err = json.Unmarshal(data, &b)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			ctx, cancel := r.config.Context()
			defer cancel()
			rowsAffected, err := r.Bulk(ctx, b)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			return c.String(
				http.StatusOK,
				strings.ReplaceAll(r.config.Format.ExecOK, "%", strconv.FormatInt(rowsAffected, 10)))
		}
	}
	restPost := func() echo.HandlerFunc {
		return func(c echo.Context) error {
			table := strings.ToUpper(c.Param("table"))

			data, err := io.ReadAll(c.Request().Body)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			var i any
			err = json.Unmarshal(data, &i)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			var records []map[string]any
			if slice, ok := i.([]any); ok {
				for _, elem := range slice {
					m, ok := elem.(map[string]any)
					if !ok {
						err := errors.Errorf("post: body %#v can not be handled", i)
						return errorResponse(c, r.config, err)
					}
					records = append(records, m)
				}
			} else if m, ok := i.(map[string]any); ok {
				records = append(records, m)
			} else {
				err := errors.Errorf("post: body %#v can not be handled", i)
				return errorResponse(c, r.config, err)
			}

			ctx, cancel := r.config.Context()
			defer cancel()
			rowsAffected, err := r.Post(ctx, table, records)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			return c.String(
				http.StatusOK,
				strings.ReplaceAll(r.config.Format.ExecOK, "%", strconv.FormatInt(rowsAffected, 10)))
		}
	}
	restPut := func() echo.HandlerFunc {
		return func(c echo.Context) error {
			table := strings.ToUpper(c.Param("table"))
			where := strings.ToUpper(c.QueryParam(r.config.Params.Where))

			data, err := io.ReadAll(c.Request().Body)
			if err != nil {
				return errorResponse(c, r.config, err)
			}
			var i any
			err = json.Unmarshal(data, &i)
			if err != nil {
				return errorResponse(c, r.config, err)
			}
			set, ok := i.(map[string]any)
			if !ok {
				err := errors.Errorf("post: body %#v can not be handled", i)
				return errorResponse(c, r.config, err)
			}

			var extraWhere []string
			for k, v := range c.QueryParams() {
				if equalsToAnyOfUpper(k, r.config.Params.Select, r.config.Params.Where, r.config.Params.Order) {
					continue
				}

				for _, vv := range v {
					var cond func(k, v string) string
					for _, cc := range r.colConds {
						if strings.HasPrefix(strings.ToUpper(vv), strings.ToUpper(cc.name)) {
							cond = cc.f
							vv = vv[len(cc.name):]
						}
					}
					if cond == nil {
						cond = func(k, v string) string {
							return fmt.Sprintf("(= .%v %v)", k, v)
						}
					}
					extraWhere = append(extraWhere, cond(k, vv))
				}
			}
			if len(extraWhere) > 0 {
				where = fmt.Sprintf("(AND %v %v)", where, strings.Join(extraWhere, ""))
			}

			ctx, cancel := r.config.Context()
			defer cancel()
			rowsAffected, err := r.Put(ctx, table, set, where)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			return c.String(
				http.StatusOK,
				strings.ReplaceAll(r.config.Format.ExecOK, "%", strconv.FormatInt(rowsAffected, 10)))
		}
	}
	restDelete := func() echo.HandlerFunc {
		return func(c echo.Context) error {
			table := strings.ToUpper(c.Param("table"))
			where := strings.ToUpper(c.QueryParam(r.config.Params.Where))

			var extraWhere []string
			for k, v := range c.QueryParams() {
				if equalsToAnyOfUpper(k, r.config.Params.Select, r.config.Params.Where, r.config.Params.Order) {
					continue
				}

				for _, vv := range v {
					var cond func(k, v string) string
					for _, cc := range r.colConds {
						if strings.HasPrefix(strings.ToUpper(vv), strings.ToUpper(cc.name)) {
							cond = cc.f
							vv = vv[len(cc.name):]
						}
					}
					if cond == nil {
						cond = func(k, v string) string {
							return fmt.Sprintf("(= .%v %v)", k, v)
						}
					}
					extraWhere = append(extraWhere, cond(k, vv))
				}
			}
			if len(extraWhere) > 0 {
				where = fmt.Sprintf("(AND %v %v)", where, strings.Join(extraWhere, ""))
			}

			ctx, cancel := r.config.Context()
			defer cancel()
			rowsAffected, err := r.Delete(ctx, table, where)
			if err != nil {
				return errorResponse(c, r.config, err)
			}

			return c.String(
				http.StatusOK,
				strings.ReplaceAll(r.config.Format.ExecOK, "%", strconv.FormatInt(rowsAffected, 10)))
		}
	}

	theURL := path.Join(r.config.Root, ":table")
	bulkURL := path.Join(r.config.Root, "!bulk")

	e := echo.New()
	e.Use(middleware.CORS())
	e.Use(middleware.Logger())
	e.Use(stacktraceMiddleware)

	e.POST(bulkURL, restBulk())

	e.GET(theURL, restGet())
	e.POST(theURL, restPost())
	e.PUT(theURL, restPut())
	e.DELETE(theURL, restDelete())

	addr := r.config.Addr
	if addr == "" {
		addr = ":12345"
	}
	e.Logger.Fatal(e.Start(addr))
}

func (r *FootREST) Get(ctx context.Context, table string, selColumns []string, whereSExpr string, orderColumns []string, rowsPerPage, page uint) ([]string, [][]any, error) {
	strStmt, args, err := r.BuildGetStmt(table, selColumns, whereSExpr, orderColumns, rowsPerPage, page)
	if err != nil {
		return nil, nil, err
	}

	rog.Debug("GET:")
	rog.Debug("  stmt=", strStmt)
	rog.Debug("  args=", args)

	dbStmt, err := r.conn.PrepareContext(ctx, strStmt)
	if err != nil {
		return nil, nil, err
	}
	defer dbStmt.Close()

	rows, err := dbStmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colnames, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var dec *encoding.Decoder
	if r.encoding != nil {
		dec = r.encoding.NewDecoder()
	}

	var rs [][]any
	for rows.Next() {
		cols := make([]any, len(colnames))
		colptrs := make([]any, len(cols))
		for i := range cols {
			colptrs[i] = &cols[i]
		}

		err = rows.Scan(colptrs...)
		if err != nil {
			return nil, nil, err
		}

		if dec != nil {
			for i := range cols {
				if s, ok := cols[i].(string); ok {
					s, err := dec.String(s)
					if err != nil {
						return nil, nil, err
					}
					cols[i] = s
				}
			}
		}

		rs = append(rs, cols)
	}

	return colnames, rs, nil
}

type manip struct {
	Method string            `json:"method"`
	Table  string            `json:"table"`
	Where  map[string]string `json:"where"`
	Values map[string]any    `json:"values"`
}
type bulk []manip

func (r *FootREST) Bulk(ctx context.Context, b bulk) (int64, error) {
	if r.conn == nil {
		return 0, nil
	}

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	ra := int64(0)

	for _, m := range b {
		where := ""
		if len(m.Where) != 0 {
			var extraWhere []string
			for k, v := range m.Where {
				var cond func(k, v string) string
				for _, cc := range r.colConds {
					if strings.HasPrefix(strings.ToUpper(v), strings.ToUpper(cc.name)) {
						cond = cc.f
						v = v[len(cc.name):]
					}
				}
				if cond == nil {
					cond = func(k, v string) string {
						return fmt.Sprintf("(= .%v %v)", k, v)
					}
				}
				extraWhere = append(extraWhere, cond(k, v))
			}
			if len(extraWhere) > 0 {
				where = fmt.Sprintf("(AND %v %v)", where, strings.Join(extraWhere, ""))
			}
		}

		var strStmt string
		var args []any

		switch strings.ToUpper(m.Method) {
		case "POST":
			strStmt, args, err = r.BuildPostStmt(m.Table, m.Values)
			if err != nil {
				return 0, err
			}
			rog.Debug("POST(Bulk):")
			rog.Debug("  stmt=", strStmt)
			rog.Debug("  args=", args)

		case "PUT":
			strStmt, args, err = r.BuildPutStmt(m.Table, m.Values, where)
			if err != nil {
				return 0, err
			}
			rog.Debug("PUT(Bulk):")
			rog.Debug("  stmt=", strStmt)
			rog.Debug("  args=", args)

		case "DELETE":
			strStmt, args, err = r.BuildDeleteStmt(m.Table, where)
			if err != nil {
				return 0, err
			}
			rog.Debug("DELETE(Bulk):")
			rog.Debug("  stmt=", strStmt)
			rog.Debug("  args=", args)
		}

		var enc *encoding.Encoder
		if r.encoding != nil {
			enc = r.encoding.NewEncoder()

			for i := range args {
				if s, ok := args[i].(string); ok {
					s, err = enc.String(s)
					if err != nil {
						return 0, err
					}
					args[i] = s
				}
			}
		}

		dbStmt, err := tx.PrepareContext(ctx, strStmt)
		if err != nil {
			return 0, err
		}
		defer dbStmt.Close()

		result, err := dbStmt.ExecContext(ctx, args...)
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}

		rra, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		ra += rra

	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return ra, nil
}

func (r *FootREST) Post(ctx context.Context, table string, values any) (int64, error) {
	strStmt, args, err := r.BuildPostStmt(table, values)
	if err != nil {
		return 0, err
	}

	rog.Debug("POST:")
	rog.Debug("  stmt=", strStmt)
	rog.Debug("  args=", args)

	var enc *encoding.Encoder
	if r.encoding != nil {
		enc = r.encoding.NewEncoder()

		for i := range args {
			if s, ok := args[i].(string); ok {
				s, err = enc.String(s)
				if err != nil {
					return 0, err
				}
				args[i] = s
			}
		}
	}

	if r.conn == nil {
		return 0, nil
	}

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	dbStmt, err := tx.PrepareContext(ctx, strStmt)
	if err != nil {
		return 0, err
	}
	defer dbStmt.Close()

	result, err := dbStmt.ExecContext(ctx, args...)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (r *FootREST) Put(ctx context.Context, table string, set map[string]any, where string) (int64, error) {
	strStmt, args, err := r.BuildPutStmt(table, set, where)
	if err != nil {
		return 0, err
	}

	rog.Debug("PUT:")
	rog.Debug("  stmt=", strStmt)
	rog.Debug("  args=", args)

	var enc *encoding.Encoder
	if r.encoding != nil {
		enc = r.encoding.NewEncoder()

		for i := range args {
			if s, ok := args[i].(string); ok {
				s, err = enc.String(s)
				if err != nil {
					return 0, err
				}
				args[i] = s
			}
		}
	}

	if r.conn == nil {
		return 0, nil
	}

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	dbStmt, err := tx.PrepareContext(ctx, strStmt)
	if err != nil {
		return 0, err
	}
	defer dbStmt.Close()

	result, err := dbStmt.ExecContext(ctx, args...)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (r *FootREST) Delete(ctx context.Context, table string, where string) (int64, error) {
	strStmt, args, err := r.BuildDeleteStmt(table, where)
	if err != nil {
		return 0, err
	}

	rog.Debug("DELETE:")
	rog.Debug("  stmt=", strStmt)
	rog.Debug("  args=", args)

	var enc *encoding.Encoder
	if r.encoding != nil {
		enc = r.encoding.NewEncoder()

		for i := range args {
			if s, ok := args[i].(string); ok {
				s, err = enc.String(s)
				if err != nil {
					return 0, err
				}
				args[i] = s
			}
		}
	}

	if r.conn == nil {
		return 0, nil
	}

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	dbStmt, err := tx.PrepareContext(ctx, strStmt)
	if err != nil {
		return 0, err
	}
	defer dbStmt.Close()

	result, err := dbStmt.ExecContext(ctx, args...)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (r *FootREST) BuildGetStmt(table string, selColumns []string, whereSExpr string, orderColumns []string, rowsPerPage, page uint) (string, []any, error) {
	table = strings.TrimSpace(table)
	whereSExpr = strings.TrimSpace(whereSExpr)

	if !r.isValidName(table) {
		return "", nil, errors.Errorf("invalid table name %q", table)
	}

	var err error
	var sc map[string]*sql.ColumnType
	if r.useSchema {
		sc, err = r.getSchema(table)
		if err != nil {
			return "", nil, errors.Wrap(err, "schema")
		}
	}

	// SELECT

	if len(selColumns) == 0 {
		selColumns = append(selColumns, "*")
	}
	for _, c := range selColumns {
		err = r.validateColumnName(c, sc)
		if err != nil {
			return "", nil, errors.Wrap(err, "validate select")
		}
	}
	selectClause := "SELECT " + strings.Join(selColumns, ", ")

	// FROM

	fromClause := "FROM " + table

	// WHERE

	whereClause := ""
	var args []any
	if whereSExpr != "" {
		var w string
		w, args, err = r.buildWhereClause(whereSExpr, sc)
		if err != nil {
			return "", nil, errors.Wrap(err, "build where")
		}
		whereClause = "WHERE " + w
	}

	// ORDER BY

	orderByClause := ""
	if len(orderColumns) != 0 {
		err = r.validateOrderByColumns(orderColumns, sc)
		if err != nil {
			return "", nil, errors.Wrap(err, "validate order")
		}

		for i, o := range orderColumns {
			if strings.HasPrefix(o, "-") {
				o = o[1:] + " DESC"
			}
			orderColumns[i] = o
		}

		orderByClause = "ORDER BY " + strings.Join(orderColumns, ", ")
	}

	pagination := r.dialect.Paginate(rowsPerPage, page)

	return strings.TrimSpace(strings.Join([]string{pagination[0], selectClause, fromClause, whereClause, orderByClause, pagination[1]}, " ")), args, nil
}

func (r *FootREST) BuildPostStmt(table string, values any) (string, []any, error) {
	table = strings.TrimSpace(table)

	if !r.isValidName(table) {
		return "", nil, errors.Errorf("invalid table name %q", table)
	}

	var err error
	var sc map[string]*sql.ColumnType
	if r.useSchema {
		sc, err = r.getSchema(table)
		if err != nil {
			return "", nil, errors.Wrap(err, "schema")
		}
	}

	var svalues []map[string]any
	if s, ok := values.([]map[string]any); ok {
		svalues = s
	} else if m, ok := values.(map[string]any); ok {
		svalues = append(svalues, m)
	} else {
		panic(fmt.Sprintf("unsupported type %T of values", values))
	}

	// normalize svalues

	allColumnMap := make(map[string]struct{})
	for si := range svalues {
		for c := range svalues[si] {
			allColumnMap[c] = struct{}{}
		}
	}
	allColumns := make([]string, 0, len(allColumnMap))
	for c := range allColumnMap {
		allColumns = append(allColumns, c)
	}
	sort.Slice(allColumns, func(i, j int) bool {
		return allColumns[i] < allColumns[j]
	})

	for si := range svalues {
		for _, c := range allColumns {
			if _, found := svalues[si][c]; !found {
				svalues[si][c] = nil
			}
		}
	}

	// INSERT INTO

	buf := bytes.NewBufferString("INSERT INTO ")
	buf.WriteString(table)
	buf.WriteString(" (")

	for i, c := range allColumns {
		if sc != nil {
			_, ok := sc[strings.ToUpper(c)]
			if !ok {
				return "", nil, errors.Errorf("column %q is not in %q scheme", c, table)
			}
		}

		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c)
	}
	buf.WriteString(") VALUES ")

	var args []any
	ph := 0

	for si := range svalues {
		if si > 0 {
			buf.WriteString(", ")
		}
		buf.WriteByte('(')

		for i, c := range allColumns {
			if i > 0 {
				buf.WriteString(", ")
			}

			buf.WriteString(r.dialect.Placeholder(ph))
			ph++
			args = append(args, svalues[si][c])
		}

		buf.WriteByte(')')
	}

	return buf.String(), args, nil
}

func (r *FootREST) BuildDeleteStmt(table string, whereSExpr string) (string, []any, error) {
	table = strings.TrimSpace(table)
	whereSExpr = strings.TrimSpace(whereSExpr)

	if !r.isValidName(table) {
		return "", nil, errors.Errorf("invalid table name %q", table)
	}

	var err error
	var sc map[string]*sql.ColumnType
	if r.useSchema {
		sc, err = r.getSchema(table)
		if err != nil {
			return "", nil, errors.Wrap(err, "schema")
		}
	}

	// DELETE

	buf := bytes.NewBufferString("DELETE FROM ")
	buf.WriteString(table)

	// WHERE

	var args []any
	if whereSExpr != "" {
		var w string
		w, args, err = r.buildWhereClause(whereSExpr, sc)
		if err != nil {
			return "", nil, errors.Wrap(err, "build where")
		}

		buf.WriteString(" WHERE ")
		buf.WriteString(w)
	}

	return buf.String(), args, nil
}

func (r *FootREST) BuildPutStmt(table string, values map[string]any, whereSExpr string) (string, []any, error) {
	table = strings.TrimSpace(table)
	whereSExpr = strings.TrimSpace(whereSExpr)

	if !r.isValidName(table) {
		return "", nil, errors.Errorf("invalid table name %q", table)
	}

	var err error
	var sc map[string]*sql.ColumnType
	if r.useSchema {
		sc, err = r.getSchema(table)
		if err != nil {
			return "", nil, errors.Wrap(err, "schema")
		}
	}

	allColumns := make([]string, 0, len(values))
	for c := range values {
		allColumns = append(allColumns, c)
	}
	sort.Slice(allColumns, func(i, j int) bool {
		return allColumns[i] < allColumns[j]
	})

	// UPDATE

	buf := bytes.NewBufferString("UPDATE ")
	buf.WriteString(table)

	// SET

	var args []any
	ph := 0

	buf.WriteString(" SET ")

	for i, c := range allColumns {
		v := values[c]

		if sc != nil {
			if _, found := sc[strings.ToUpper(c)]; !found {
				return "", nil, errors.Errorf("column %q is not in %q scheme", c, table)
			}
		}

		if i > 0 {
			buf.WriteString(", ")
		}

		buf.WriteString(c)
		buf.WriteString(" = ")
		buf.WriteString(r.dialect.Placeholder(ph))
		ph++
		args = append(args, v)
	}

	// WHERE

	if whereSExpr != "" {
		var w string
		w, wargs, err := r.buildWhereClause(whereSExpr, sc)
		if err != nil {
			return "", nil, errors.Wrap(err, "where")
		}

		buf.WriteString(" WHERE ")
		buf.WriteString(w)
		args = append(args, wargs...)
	}

	return buf.String(), args, nil
}

func (r *FootREST) buildWhereClause(w string, sc map[string]*sql.ColumnType) (whereClause string, args []any, err error) {
	syntax := &sexpr.Syntax{
		// A set of list delimiters. These are pairs of strings denoting the
		// start and end of an S-expression.
		// E.g.: "(", ")"
		Delimiters: [][2]string{{"(", ")"}},

		// This string starts a single line comment.
		// A single line comment runs until the end of a line.
		// E.g: "//"
		//SingleLineComment: "",

		// These strings denote what a multi-line comment starts with
		// and ends with.
		// E.g.: "/*", "*/"
		//MultiLineComment: []string{},

		// These strings determine how a string literal starts and ends.
		// E.g.: "abc".
		StringLit: []string{`'`, `'`},

		// These strings determine how a raw string literal starts and ends.
		// A raw string does not have its escape sequences parsed.
		// E.g.: `abc`.
		//RawStringLit: []string{},

		// These strings determine how a char literal starts and ends.
		// E.g.: 'a'.
		//CharLit: []string{},

		// This function should return whether or not the given
		// input qualifies as a boolean.
		BooleanFunc: func(l *sexpr.Lexer) int {
			if ret := l.AcceptLiteral("TRUE"); ret != 0 {
				return ret
			}
			return l.AcceptLiteral("FALSE")
		},

		// This function should return whether or not the given
		// input qualifies as a number.
		NumberFunc: sexpr.LexNumber, // func(l *sexpr.Lexer) int {},
	}

	var ast sexpr.AST
	err = sexpr.ParseString(&ast, w, syntax)
	if err != nil {
		return "", nil, err
	}
	if len(ast.Root.Children) == 0 {
		return "", nil, errors.New("no children")
	}

	//rog.Debug(ast.String())

	//defer ast.ReleaseNodes()

	phnum := 0
	return r.buildWhereClauseInner(ast.Root.Children[0], &phnum, sc)
}

func (r *FootREST) buildWhereClauseInner(node *sexpr.Node, phnum *int, sc map[string]*sql.ColumnType) (string, []any, error) {
	if phnum == nil {
		panic("phnum is nil")
	}

	if len(node.Children) == 0 {
		return "", nil, errors.New("invalid expr")
	}

	car := node.Children[0]
	cdr := node.Children[1:]

	if car.Type != sexpr.TokIdent {
		return "", nil, errors.Errorf("%q is not an operator", car.Data)
	}

	operatorName := strings.ToUpper(string(car.Data))
	operator, found := r.dialect.Operators[operatorName]
	if !found {
		return "", nil, errors.Errorf("operator %q is not registered", operatorName)
	}
	if operator.Format == "" {
		operator.Format = strings.ReplaceAll(DefaultOperatorFormat, "{OPERATOR}", operatorName)
	}

	var enc *encoding.Encoder
	if r.encoding != nil {
		enc = r.encoding.NewEncoder()
	}

	args := []any{}
	subww := []string{}
	for i, c := range cdr {
		data := string(c.Data)

		if c.Type == sexpr.TokListOpen {
			subw, suba, err := r.buildWhereClauseInner(c, phnum, sc)
			if err != nil {
				return "", nil, err
			}
			subww = append(subww, subw)
			args = append(args, suba...)

		} else if strings.HasPrefix(data, ".") {
			data = data[1:]
			if !r.isValidName(data) {
				return "", nil, errors.Errorf("invalid column name %q", data)
			}
			if sc != nil {
				data := strings.ToUpper(data)
				if _, ok := sc[data]; !ok {
					return "", nil, errors.Errorf("invalid column name %q", data)
				}
			}
			subww = append(subww, data)

		} else {
			subww = append(subww, r.dialect.Placeholder(*phnum))
			*phnum++

			if c.Type == sexpr.TokString {
				if enc != nil {
					s, err := enc.String(data)
					if err != nil {
						return "", nil, err
					}
					data = s
				}
				args = append(args, r.dialect.Arg(len(args), data))
			} else {
				var typ *sql.ColumnType
				if sc != nil {
					for ii, cc := range cdr {
						if ii == i {
							continue
						}
						ccname := string(cc.Data)
						if !strings.HasPrefix(ccname, ".") {
							continue
						}
						ccname = ccname[1:]
						if t, ok := sc[ccname]; ok {
							typ = t
							break
						}
					}
				}
				value, err := conv(data, typ)
				if err != nil {
					return "", nil, err
				}
				args = append(args, r.dialect.Arg(len(args), value))
			}
		}
	}
	w, err := operator.ApplyFormat(subww...)
	if err != nil {
		return "", nil, err
	}

	return w, args, nil
}

func (r *FootREST) getSchema(table string) (map[string]*sql.ColumnType, error) {
	if r.conn == nil {
		return nil, nil
	}

	table = strings.ToUpper(table)

	r.scMut.Lock()
	if sc, found := r.schemaCache[table]; found {
		r.scMut.Unlock()
		return sc, nil
	}
	r.scMut.Unlock()

	stmt, err := r.conn.Prepare("SELECT * FROM " + table + " WHERE 1=0 ")
	if err != nil {
		return nil, errors.Wrap(err, "prepare")
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "column types")
	}

	scMap := make(map[string]*sql.ColumnType)
	for _, typ := range types {
		scMap[strings.ToUpper(typ.Name())] = typ
	}

	r.scMut.Lock()
	if r.schemaCache == nil {
		r.schemaCache = make(map[string](map[string]*sql.ColumnType))
	}
	r.schemaCache[table] = scMap
	r.scMut.Unlock()

	return scMap, nil
}

func conv(s string, typ *sql.ColumnType) (any, error) {

	if strings.HasPrefix(s, "-") {
		return strconv.Atoi(s)
	}
	if strings.HasPrefix(s, "#") {
		return strconv.Atoi(s[1:])
	}

	t := strings.ToUpper(s)
	if t == "NULL" {
		return nil, nil
	}
	if t == "TRUE" {
		return true, nil
	}
	if t == "FALSE" {
		return false, nil
	}

	if typ != nil {
		if _, scale, ok := typ.DecimalSize(); ok {
			if scale > 0 {
				return strconv.ParseFloat(s, 64)
			}
			return strconv.Atoi(s)
		}

		if _, ok := typ.Length(); ok {
			return s, nil
		}

		if strings.Contains(typ.DatabaseTypeName(), "INT") {
			//
			return strconv.Atoi(s)
		}

		if strings.Contains(typ.DatabaseTypeName(), "DEC") ||
			strings.Contains(typ.DatabaseTypeName(), "NUM") ||
			strings.Contains(typ.DatabaseTypeName(), "FLOAT") ||
			strings.Contains(typ.DatabaseTypeName(), "REAL") {
			//
			return strconv.ParseFloat(s, 64)
		}

		if strings.Contains(typ.DatabaseTypeName(), "BOOL") {
			//
			return strconv.Atoi(s)
		}
	}

	return s, nil
}

//var validDBIdentifier *regexp.Regexp = regexp.MustCompile(`^([[:alnum:]]|_)+$`)

//var validDBIDRanges = []*unicode.RangeTable{unicode.Letter, unicode.Digit, &unicode.RangeTable{
//	R16: []unicode.Range16{
//		{0x005f, 0x005f, 1},
//	},
//}}

func (r *FootREST) isValidName(name string) bool {
	if f := r.dialect.IsValidName; f != nil {
		return f(name)
	}
	return true
}

func (r *FootREST) validateColumnName(name string, sc map[string]*sql.ColumnType) error {
	name = strings.TrimSpace(strings.ToUpper(name))

	if name == "*" {
		return nil
	}

	if !r.isValidName(name) {
		return errors.Errorf("invalid column name %q", name)
	}

	if sc != nil {
		if _, found := sc[name]; !found {
			return errors.Errorf("column %q not in a schema", name)
		}
	}

	return nil
}

func (r *FootREST) validateOrderByColumns(cols []string, sc map[string]*sql.ColumnType) error {
	if len(cols) == 0 {
		return nil
	}

	for _, c := range cols {
		c = strings.ToUpper(c)
		c = strings.TrimPrefix(c, "-")

		if !r.isValidName(c) {
			return errors.Errorf("col %q: invalid name", c)
		}

		if sc != nil {
			found := false
			for _, typ := range sc {
				if c == strings.ToUpper(typ.Name()) {
					found = true
				}
			}
			if !found {
				return errors.Errorf("col %q: not found", c)
			}
		}
	}

	return nil
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

func equalsToAnyOfUpper(s1 string, ss ...string) bool {
	s1 = strings.ToUpper(s1)
	for _, s := range ss {
		s = strings.ToUpper(s)
		if s1 == s {
			return true
		}
	}
	return false
}

func errorResponse(c echo.Context, config Config, err error) error {
	_ = c.String(http.StatusInternalServerError, strings.ReplaceAll(config.Format.Error, "%", `"`+escape(err.Error())+`"`))
	return err
}

func stacktraceMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		var err error
		if err = next(c); err != nil {
			log.Printf("%v\n", stacktrace.New(err))
			c.Error(err)
		}
		return err
	}
}
