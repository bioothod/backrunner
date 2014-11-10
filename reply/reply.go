package reply

import "github.com/bioothod/elliptics-go/elliptics"

type Entry struct {
	Get    string		`json:"get"`
	Update string		`json:"update"`
	Delete string		`json:"delete"`
	Key    string		`json:"key"`
}

type LookupServerResult struct {
	IDString	string			`json:"id"`
	CsumString	string			`json:"csum"`
	Filename	string			`json:"filename"`
	Size		uint64			`json:"size"`
	Offset		uint64			`json:"offset-within-data-file"`
	MtimeString	string			`json:"mtime"`
	ServerString	string			`json:"server"`
	Error		*elliptics.DnetError	`json:"error"`

	Server		*elliptics.DnetAddr	`json:"-"`
	Info		*elliptics.DnetFileInfo	`json:"-"`
}

type LookupResult struct {
	Servers		[]*LookupServerResult	`json:"info"`
	SuccessGroups	[]uint32		`json:"success-groups"`
	ErrorGroups	[]uint32		`json:"error-groups"`
}

type Upload struct {
	Bucket  string				`json:"bucket"`
	Primary Entry				`json:"primary"`
	Reply   *LookupResult			`json:"reply"`
}
