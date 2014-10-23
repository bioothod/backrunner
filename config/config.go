package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Formatter struct {
	Type string				`json:"type"`
	Pattern string				`json:"pattern"`
}
type Rotation struct {
	Move int				`json:"move"`
}
type Sink struct {
	Type string				`json:"type"`
	Path string				`json:"path"`
	AutoFlush bool				`json:"autoflush"`
	Rotation Rotation			`json:"rotation"`
}
type Frontend struct {
	Formatter Formatter			`json:"formatter"`
	Sink Sink				`json:"sink"`
}
type Logger struct {
	Frontends []Frontend			`json:"frontends"`
	Level string				`json:"level"`
}
type Monitor struct {
	Port int32				`json:"port"`
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
	Monitor Monitor				`json:"monitor"`
}

type Backend struct {
	Backend_ID uint32			`json:"backend_id"`
	Type string				`json:"type"`
	Group uint32				`json:"group"`
	History string				`json:"history"`
	Data string				`json:"data"`
	Sync int				`json:"sync"`
	Blob_Flags uint64			`json:"blob_flags"`
	Blob_Size string			`json:"blob_size"`
	Records_In_Blob uint64			`json:"records_in_blob"`
	Blob_Size_Limit string			`json:"blob_size_limit"`
	DefragPercentage int			`json:"defrag_percentage"`
}
type EllipticsServerConfig struct {
	Logger Logger				`json:"logger"`
	Options Options				`json:"options"`
	Backends []Backend			`json:"backends"`

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

type EllipticsClientConfig struct {
	LogFile string				`json:"log-file"`
	LogLevel string				`json:"log-level"`
	LogPrefix string			`json:"log-prefix"`
	Remote []string				`json:"remote"`
	MetadataGroups []uint32			`json:"metadata-groups"`
}

type ProxyClientConfig struct {
	Address string				`json:"address"`
	// http connection timeout in seconds
	IdleTimeout int				`json:"idle-timeout"`

	// minimum available free space ratio of bucket to be writable
	MinAvailSpaceRatio float64		`json:"min-avail-space-ratio"`

	// bucket metadata update time in seconds
	BucketUpdateInterval int		`json:"bucket-update-interval"`

	// bucket statistics update time in seconds
	BucketStatUpdateInterval int		`json:"bucket-stat-update-interval"`

	// all redirect requests will be redirected to following port, it port is outside of allowed [0, 65536) range,
	// redirect requests will return http.StatusServiceUnavailable
	RedirectPort int			`json:"redirect-port"`

	// all redirect replies will contain @auth.AuthHeaderStr 'Authorization' header generated with given token
	RedirectToken string

	// number of seconds redirect signature is valid since @Signtime, streaming module will not return data if timeout has passed
	RedirectSignatureTimeout int		`json:"redirect-signature-timeout"`
}

type ProxyConfig struct {
	Elliptics EllipticsClientConfig		`json:"elliptics"`
	Proxy ProxyClientConfig			`json:"proxy"`
}

func (config *ProxyConfig) Save(file string) (err error) {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("Could not marshal elliptics transport config structure: %v", err)
	}

	err = ioutil.WriteFile(file, data, 0644)
	if err != nil {
		return fmt.Errorf("Could not write file '%s': %v", file, err)
	}

	return
}

func (config *ProxyConfig) Load(file string) (err error) {
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
