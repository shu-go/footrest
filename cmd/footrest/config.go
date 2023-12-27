package main

import "github.com/shu-go/footrest"

type Config struct {
	footrest.Config

	DBType     string
	Connection string

	Debug bool
}

func defaultConfig() *Config {
	fconfig := footrest.DefaultConfig()
	if fconfig == nil {
		return nil
	}

	return &Config{
		Config: *fconfig,
		//DBType: ,
		//Connection: ,
		Debug: false,
	}
}
