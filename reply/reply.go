package reply

type Entry struct {
	Get    string		`json:"get"`
	Update string		`json:"update"`
	Delete string		`json:"delete"`
	Key    string		`json:"key"`
}

type Upload struct {
	Bucket  string				`json:"bucket"`
	Primary Entry				`json:"primary"`
	Reply   *map[string]interface{}		`json:"reply"`
}
