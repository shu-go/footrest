package footrest

import (
	"context"
	"time"
)

type Config struct {
	Format ResponseFormat
	Params SpecialParams

	Timeout int64

	Addr string
	Root string
}

type ResponseFormat struct {
	QueryOK string
	ExecOK  string
	Error   string
}

type SpecialParams struct {
	Select string
	Where  string
	Upsert string
	Order  string
	Rows   string
	Page   string
}

func DefaultConfig() *Config {
	return &Config{
		Format: ResponseFormat{
			QueryOK: `{"result": [%]}`,
			ExecOK:  `{"result": %}`,
			Error:   `{"error": %}`,
		},
		Params: SpecialParams{
			Select: "select",
			Where:  "where",
			Upsert: "upsert",
			Order:  "order",
			Rows:   "rows",
			Page:   "page",
		},

		Timeout: int64(5 * time.Second / time.Millisecond),

		Addr: ":12345",
		Root: "/",
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
