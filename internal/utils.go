package internal

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"unsafe"
)

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func FileSize(path string) (int64, error) {
	f, e := os.Stat(path)
	if e != nil {
		return 0, e
	}
	return f.Size(), nil
}

func LoadConf(path string, cf interface{}) bool {
	filesize, err := FileSize(path)
	if err != nil || filesize < 1 {
		log.Println(err.Error())
		return false
	}
	buf := make([]byte, filesize)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		log.Println(err.Error())
		return false
	}
	n, err := f.Read(buf)
	if err != nil || filesize != int64(n) {
		log.Println(err.Error())
		return false
	}
	if err := UnmarshalJson(buf, cf); err != nil {
		return false
	}
	return true
}

func UnmarshalJson(jsonVal []byte, objVal interface{}) error {
	decoder := json.NewDecoder(strings.NewReader(BytesToString(jsonVal)))
	decoder.UseNumber()
	return decoder.Decode(objVal)
}
