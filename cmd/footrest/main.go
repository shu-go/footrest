package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/text/encoding"

	"github.com/shu-go/gli/v2"
	"github.com/shu-go/rog"

	"github.com/shu-go/footrest"

	_ "github.com/shu-go/footrest/dialect/sqlite"
	_ "modernc.org/sqlite"

	_ "github.com/shu-go/footrest/dialect/oracle"
	_ "github.com/sijms/go-ora/v2"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/shu-go/footrest/dialect/sqlserver"
)

// Version is app version
var Version string

type globalCmd struct {
	Generate genCmd `cli:"generate,gen"`
}

func (c globalCmd) Run(args []string) error {
	configFileName := "footrest.config"
	if len(args) > 0 {
		configFileName = args[0]
	}
	config, err := loadConfig(configFileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		config = defaultConfig()
	}

	if config.Debug {
		rog.EnableDebug()
		defer rog.DisableDebug()
	}

	rog.Debug("type", config.DBType)
	rog.Debug("conn", config.Connection)
	rog.Debug("addr", config.Addr)
	rog.Debug("root", config.Root)

	var enc encoding.Encoding
	r, conn, err := footrest.NewConn(config.DBType, config.Connection, enc, true, &config.Config)
	if err != nil {
		return err
	}
	defer conn.Close()

	r.Serve()

	return nil
}

type genCmd struct{}

func (c genCmd) Run(args []string) error {
	configFileName := "footrest.config"
	if len(args) > 0 {
		configFileName = args[0]
	}

	config := defaultConfig()

	return saveConfig(configFileName, *config)
}

func loadConfig(name string) (*Config, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	config := defaultConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func saveConfig(name string, config Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(name, data, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	app := gli.NewWith(&globalCmd{})
	app.Name = "footrest"
	app.Desc = ""
	app.Version = Version
	app.Usage = ``
	app.Copyright = "(C) 2022 Shuhei Kubota"
	app.SuppressErrorOutput = true
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

}
