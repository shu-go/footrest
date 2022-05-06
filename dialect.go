package footrest

import (
	"strconv"
	"strings"
	"sync"
)

//////////////////////////////////////////////////

func init() {
	d := DefaultDialect()
	RegisterDialect("", &d)
}

//

var (
	dMut       sync.Mutex
	dialectMap map[string]*Dialect
)

// RegisterDialect add *Dialect d into a global map.
//
// See doc about Dialect.
func RegisterDialect(name string, d *Dialect) {
	dMut.Lock()
	if dialectMap == nil {
		dialectMap = make(map[string]*Dialect)
	}
	dialectMap[name] = d
	dMut.Unlock()
}

func GetDialect(name string) *Dialect {
	dMut.Lock()
	d, found := dialectMap[name]
	if !found {
		d, _ = dialectMap[""]
	}
	dMut.Unlock()

	return d
}

//////////////////////////////////////////////////

// The best way to define a dialect is use DefaultDialect() and tweek the returned dialect.
type Dialect struct {
	Operators   map[string]Operator // key(Operator.Name) must be upper case.
	Placeholder func(int) string
}

func (d *Dialect) AddOperator(name string, format string, f ...OperatorFormatter) {
	o := Operator{}

	o.Name = name

	o.Format = format
	if format == "" {
		o.Format = strings.ReplaceAll(DefaultOperatorFormat, "{OPERATOR}", name)
	}

	if len(f) > 0 {
		o.Formatter = f[0]
	}

	if d.Operators == nil {
		d.Operators = make(map[string]Operator)
	}
	d.Operators[strings.ToUpper(name)] = o
}

func DefaultDialect() Dialect {
	d := Dialect{}

	d.AddOperator("==", "")
	d.AddOperator("=", "")
	d.AddOperator("!=", "")
	d.AddOperator("<>", "")
	d.AddOperator(">", "")
	d.AddOperator("<", "")
	d.AddOperator(">=", "")
	d.AddOperator("<=", "")
	d.AddOperator("!<", "")
	d.AddOperator("!>", "")
	d.AddOperator("LIKE", "")
	d.AddOperator("BETWEEN", "$1 BETWEEN $2 AND $3")

	d.AddOperator("IS", "")
	d.AddOperator("ISNOT", "$1 IS NOT $2")
	d.AddOperator("ISNULL", "$1 IS NULL")
	d.AddOperator("ISNOTNULL", "$1 IS NOT NULL")

	d.AddOperator("NOT", "NOT ($1)")

	d.AddOperator("AND", "($1) AND ($2)", func(args ...string) (string, error) {
		myargs := make([]string, len(args))
		for i := range args {
			myargs[i] = "(" + args[i] + ")"
		}
		return strings.Join(myargs, " AND "), nil
	})
	d.AddOperator("OR", "($1) OR ($2)", func(args ...string) (string, error) {
		myargs := make([]string, len(args))
		for i := range args {
			myargs[i] = "(" + args[i] + ")"
		}
		return strings.Join(myargs, " OR "), nil
	})

	d.AddOperator("||", "")

	d.Placeholder = func(int) string {
		return "?"
	}

	return d
}

const DefaultOperatorFormat = `$1 {OPERATOR} $2`

type OperatorFormatter func(args ...string) (string, error)

type Operator struct {
	Name      string
	Format    string            // "$1 == $2", "$1 BETWEEN $2 AND $3"
	Formatter OperatorFormatter // optional
}

func (o Operator) ApplyFormat(args ...string) (string, error) {
	if o.Formatter != nil {
		return o.Formatter(args...)
	}

	result := o.Format
	for i, a := range args {
		result = strings.ReplaceAll(result, "$"+strconv.Itoa(i+1), a)
	}
	return result, nil
}
