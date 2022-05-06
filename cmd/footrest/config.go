package main

import "github.com/shu-go/footrest"

type Config struct {
	footrest.Config

	DBType     string
	Connection string

	ShiftJIS bool

	Debug bool
}

func defaultConfig() *Config {
	return &Config{
		Config: *(footrest.DefaultConfig()),
		//DBType: ,
		//Connection: ,
		ShiftJIS: false,
		Debug:    false,
	}
}
