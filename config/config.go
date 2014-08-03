package config

import (
	"encoding/json"
	"io/ioutil"
)

func Parse(file string) (js interface{}, err error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, &js)
	if err != nil {
		return
	}

	return
}
