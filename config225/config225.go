package config225

import (
	"encoding/json"
	"fmt"
	cnf "github.com/bioothod/backrunner/config"
	"io/ioutil"
)

type Formatter struct {
	Type string				`json:"type"`
	Pattern string				`json:"pattern"`
}
type Sink struct {
	Type string				`json:"type"`
	Path string				`json:"path"`
	AutoFlush bool				`json:"autoflush"`
}
type Root struct {
	Formatter Formatter			`json:"formatter"`
	Sink Sink				`json:"sink"`
}
type Loggers struct {
	Type string				`json:"type"`
	Level int				`json:"level"`
	Root []Root				`json:"root"`
}
type Options struct {
	Join bool				`json:"join"`
	Flags uint64				`json:"flags"`
	Remote []string				`json:"remote"`
	Address []string			`json:"address"`
	Wait_Timeout uint64			`json:"wait_timeout"`
	Check_Timeout uint64			`json:"check_timeout"`
	NonBlocking_IO_Thread_Num int		`json:"nonblocking_io_thread_num"`
	IO_Thread_Num int			`json:"io_thread_num"`
	Net_Thread_Num int			`json:"net_thread_num"`
	Daemon bool				`json:"daemon"`
	Auth_Cookie string			`json:"auth_cookie"`
	MonitorPort int				`json:"monitor_port"`
}

type EllipticsServerConfig struct {
	Loggers Loggers				`json:"loggers"`
	Options Options				`json:"options"`
	Backends []cnf.Backend			`json:"backends"`
}

func (config *EllipticsServerConfig) Save(file string) (err error) {
	data, err := json.MarshalIndent(config, "", "	")
	if err != nil {
		return fmt.Errorf("Could not marshal elliptics transport config structure: %v", err)
	}

	err = ioutil.WriteFile(file, data, 0644)
	if err != nil {
		return fmt.Errorf("Could not write file '%s': %v", file, err)
	}

	return
}

func (config *EllipticsServerConfig) Load(file string) (err error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		return
	}

	return
}

