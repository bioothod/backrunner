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
}

type ProxyClientConfig struct {
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
	RedirectToken string

	// number of seconds redirect signature is valid since @Signtime, streaming module will not return data if timeout has passed
	RedirectSignatureTimeout int		`json:"redirect-signature-timeout"`

	// cut this substring from the beginning of the filename sent to the streaming module
	RedirectRoot string			`json:"redirect-root"`

	// headers added to every reply in a 'key: value' format
	Headers map[string]string		`json:"headers"`

	// rate of free space in given backend to start scanning for defragmentation
	// i.e. only if free space rate is less than @DefragFreeSpaceLimit, given backend can be considered for defragmentation
	// defragmentation is limited by number of backends per server, number of buckets per cluster and so on
	// defragmentation percentage in the low-level backend is still considered when defrag starts
	// this caonfig parameter only specifies backends which could be defragmented, but it doesn't mean they will be
	DefragFreeSpaceLimit float64		`json:"defrag-free-space-limit"`

	// only run defragmentation in backends which will free at least @DefragRemovedSpaceLimit byte ratio
	DefragRemovedSpaceLimit float64		`json:"defrag-removed-space-limit"`

	// maximum number of buckets where defragmentation is allowed to run in parallel
	DefragMaxBuckets int			`json:"defrag-max-buckets"`

	// maximum number of backends being defragmented on any single server node
	DefragMaxBackendsPerServer int		`json:"defrag-max-backends-per-server"`

	// all URL path strings which do not match registered handlers are being read
	// as static objects living in @Root directory
	// for example requesting http://example.com/crossdomain.xml URL
	// will return @Root/crossdomain.xml file
	// directory listing is not supported
	Root string				`json:"root"`
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
