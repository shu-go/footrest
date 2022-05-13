package footrest

import (
	"context"
	"time"
)

type Config struct {
	Format  ResponseFormat
	Timeout int64

	Addr string
	Root string

	ParamSelect string
	ParamWhere  string
	ParamOrder  string
	ParamRows   string
	ParamPage   string
}

type ResponseFormat struct {
	QueryOK string
	ExecOK  string
	Error   string
}

func DefaultConfig() *Config {
	return &Config{
		Format: ResponseFormat{
			QueryOK: `{"result": [%]}`,
			ExecOK:  `{"result": %}`,
			Error:   `{"error": %}`,
		},
		Timeout: int64(5 * time.Second / time.Millisecond),

		Addr: ":12345",
		Root: "/",

		ParamSelect: "select",
		ParamWhere:  "where",
		ParamOrder:  "order",
		ParamRows:   "rows",
		ParamPage:   "page",
	}
}

func (c Config) Context() (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	ctx := context.Background()
	if c.Timeout >= 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(c.Timeout)*time.Millisecond)
	} else {
		cancel = func() {}
	}
	return ctx, cancel
}
