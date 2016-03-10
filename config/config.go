package config

import (
	"encoding/json"
	"fmt"
	"github.com/bioothod/elliptics-go/elliptics"
	"io"
	"io/ioutil"
	"os"
)

var (
	BuildDate	string
	LastCommit	string
	EllipticsGoLastCommit	string
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
	CallTreeTimeout int32			`json:"call_tree_timeout"`
}
type Cache struct {
	Size uint64				`json:"size"`
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
	//Cache Cache				`json:"cache"`
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
	PeriodicTimeout int			`json:"periodic_timeout"`
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

	// when present, backrunner reads its proxy config from elliptics from metadata groups
	BackrunnerConfig string			`json:"backrunner-config-key"`

	// when present, backrunner reads list of buckets from elliptics
	BucketList string			`json:"bucket-list-key"`

	// elliptics node config parameters like number of IO threads, various timeouts, flags and so on
	Node elliptics.NodeConfig		`json:"node-config"`
}

type ProxyClientConfig struct {
	// address to listen for incomming connections
	// if it is empty, TLS connections can still be accepted (see below @HTTPSAddress parameter)
	Address string				`json:"address"`

	// http connection timeout in seconds
	IdleTimeout int				`json:"idle-timeout"`

	// minimum available free space ratio of bucket to be writable
	// but if there are no other options, proxy will select among buckets,
	// which have more than hard ratio limit but less than soft ratio limit of free space
	FreeSpaceRatioSoft float64		`json:"free-space-ratio-soft"`

	// it is **really** forbidden to write into the bucket which has less than hard limit of free space
	FreeSpaceRatioHard float64		`json:"free-space-ratio-hard"`

	// bucket metadata update time in seconds
	BucketUpdateInterval int		`json:"bucket-update-interval"`

	// bucket statistics update time in seconds
	BucketStatUpdateInterval int		`json:"bucket-stat-update-interval"`

	// all redirect requests will be redirected to following port, it port is outside of allowed [0, 65536) range,
	// redirect requests will return http.StatusServiceUnavailable
	RedirectPort int			`json:"redirect-port"`

	// all redirect replies will contain @auth.AuthHeaderStr 'Authorization' header generated with given token
	RedirectToken string			`json:"redirect-token"`

	// number of seconds redirect signature is valid since @Signtime, streaming module will not return data if timeout has passed
	RedirectSignatureTimeout int		`json:"redirect-signature-timeout"`

	// cut this substring from the beginning of the filename sent to the streaming module
	RedirectRoot string			`json:"redirect-root"`

	// headers added to every reply in a 'key: value' format
	Headers map[string]string		`json:"headers"`

	// all URL path strings which do not match registered handlers are being read
	// as static objects living in @Root directory
	// for example requesting http://example.com/crossdomain.xml URL
	// will return @Root/crossdomain.xml file
	// directory listing is not supported
	Root string				`json:"root"`

	// HTTPS listen address
	// when specified, there must be also specified CertFile and KeyFile parameters
	// when set, server will listen on this address for incomming TLS connections
	HTTPSAddress string			`json:"https_address"`

	// certificate file for HTTPS server
	CertFile string				`json:"cert_file"`

	// Key file for HTTPS server
	KeyFile string				`json:"key_file"`

	ContentTypes map[string]string		`json:"content-types"`

	// Reader/Writer elliptics IO flags
	ReaderIOFlags uint32			`json:"reader-io-flags"`
	WriterIOFlags uint32			`json:"writer-io-flags"`

	DisableConfigUpdateForSeconds uint32	`json:"disable-config-update-for-seconds"`
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

func (config *ProxyConfig) LoadIO(in io.Reader) (err error) {
	data, err := ioutil.ReadAll(in)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		return
	}

	return
}

func (config *ProxyConfig) Load(file string) error {
	io, err := os.Open(file)
	if err != nil {
		return err
	}
	defer io.Close()

	return config.LoadIO(io)
}
