package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	db "../hustdb/handler"
	"../internal/utils"
)

type Result struct {
	data    []byte
	status  uint32
	integer int
	array   []string
}

const (
	successStatus = 0x01
	nilStatus     = 0x02
	integerStatus = 0x04
	arrayStatus   = 0x10
	errStatus     = 0x20
)

type CheckFunc func(args [][]byte) error
type HandleFunc func(args [][]byte) *Result

type CmdHandler struct {
	cmdName    string
	minParams  int
	maxParams  int
	handleFunc HandleFunc
	checkFunc  CheckFunc
}

func (this *CmdHandler) check(args [][]byte) error {
	argc := len(args)
	if argc < this.minParams || (this.maxParams > 0 && argc > this.maxParams) {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command", this.cmdName)
	}
	return nil
}

func NewCmdHandler(cmdName string, minParams, maxParams int, check CheckFunc, handle HandleFunc) *CmdHandler {
	return &CmdHandler{
		cmdName:    cmdName,
		minParams:  minParams,
		maxParams:  maxParams,
		checkFunc:  check,
		handleFunc: handle,
	}
}

var (
	CmdMap = map[string]*CmdHandler{
		"set":           NewCmdHandler("set", 3, 0, nil, setHandle),
		"get":           NewCmdHandler("get", 2, 2, nil, getHandle),
		"exists":        NewCmdHandler("exists", 2, 2, nil, existsHandle),
		"del":           NewCmdHandler("del", 2, 0, nil, delHandle),
		"strlen":        NewCmdHandler("strlen", 2, 2, nil, strlenHandle),
		"hdel":          NewCmdHandler("hdel", 3, 0, nil, hdelHandle),
		"hexists":       NewCmdHandler("hexists", 3, 3, nil, hexistsHandle),
		"hget":          NewCmdHandler("hget", 3, 3, nil, hgetHandle),
		"hincrby":       NewCmdHandler("hincrby", 4, 4, nil, hincrbyHandle),
		"hset":          NewCmdHandler("hset", 4, 4, nil, hsetHandle),
		"sadd":          NewCmdHandler("sadd", 3, 0, nil, saddHandle),
		"sismember":     NewCmdHandler("sismember", 3, 3, nil, sismemberHandle),
		"srem":          NewCmdHandler("srem", 3, 0, nil, sremHandle),
		"zadd":          NewCmdHandler("zadd", 4, 0, nil, zaddHandle),
		"zrange":        NewCmdHandler("zrange", 4, 5, nil, zrangeHandle),
		"zrangebyscore": NewCmdHandler("zrangebyscore", 4, 0, nil, zrangeByScoreHandle),
		"zrem":          NewCmdHandler("zrem", 3, 0, nil, zremHandle),
		"zscore":        NewCmdHandler("zscore", 3, 3, nil, zscoreHandle),
		"zincrby":       NewCmdHandler("zincrbyHandle", 4, 4, nil, zincrbyHandle),

		"hlen":  NewCmdHandler("hlen", 2, 2, nil, hlenHandle),
		"rpush": NewCmdHandler("rpushHandle", 3, 0, nil, nil),
		"lpop":  NewCmdHandler("lpopHandle", 3, 0, nil, nil),
		"llen":  NewCmdHandler("llenHandle", 3, 0, nil, nil),

		"echo": NewCmdHandler("echo", 2, 2, nil, echoHandle),
		"ping": NewCmdHandler("ping", 1, 1, nil, pingHandle),
	}
	// IDBHandle = &DBHandle{}
	IDBHandle = db.NewHustdbHandler()
)

func setHandle(args [][]byte) *Result {
	argc := len(args)
	params := map[string][]byte{
		"key": args[1],
	}
	checkNxXx := func(pos int) *Result {
		arg := bytes.ToLower(args[pos])
		if bytes.Compare(arg, []byte("nx")) == 0 {
			resp := IDBHandle.HustdbExist(params)
			if resp.Code == 200 {
				return &Result{
					status: nilStatus,
				}
			}
		} else if bytes.Compare(arg, []byte("xx")) == 0 {
			resp := IDBHandle.HustdbExist(params)
			if resp.Code != 200 {
				return &Result{
					status: nilStatus,
				}
			}
		} else {
			return &Result{
				status: errStatus,
				data:   []byte("ERR syntax error"),
			}
		}
		return nil
	}
	checkTTL := func(pos int) *Result {
		arg := bytes.ToLower(args[pos])
		if bytes.Compare(arg, []byte("ex")) == 0 {
			_, err := strconv.ParseInt(utils.BytesToString(args[pos+1]), 10, 64)
			if err != nil {
				return &Result{
					status: errStatus,
					data:   []byte("ERR value is not an integer or out of range"),
				}
			}
			params["ttl"] = args[pos+1]
		} else if bytes.Compare(arg, []byte("px")) == 0 {
			val, err := strconv.ParseInt(utils.BytesToString(args[pos+1]), 10, 64)
			if err != nil {
				return &Result{
					status: errStatus,
					data:   []byte("ERR value is not an integer or out of range"),
				}
			}
			val = val / 1000
			if val == 0 {
				val = 1
			}
			params["ttl"] = []byte(strconv.FormatInt(val, 10))
		} else {
			return &Result{
				status: errStatus,
				data:   []byte("ERR syntax error"),
			}
		}
		return nil
	}

	for i := 3; i < argc; i++ {
		arg := bytes.ToLower(args[i])
		if bytes.Compare(arg, []byte("nx")) == 0 || bytes.Compare(arg, []byte("xx")) == 0 {
			if result := checkNxXx(i); result != nil {
				return result
			}
		} else if bytes.Compare(arg, []byte("ex")) == 0 || bytes.Compare(arg, []byte("px")) == 0 {
			//At least ex have one argument
			if i >= argc-1 {
				return &Result{
					status: errStatus,
					data:   []byte("ERR syntax error"),
				}
			} else {
				if result := checkTTL(i); result != nil {
					return result
				}
				i += 1
			}

		} else {
			return &Result{
				status: errStatus,
				data:   []byte("ERR syntax error"),
			}
		}
	}
	params["val"] = args[2]
	resp := IDBHandle.HustdbPut(params)
	if resp.Code == 200 {
		return &Result{
			status: successStatus,
			data:   []byte("OK"),
		}
	} else {
		return &Result{
			status: nilStatus,
		}
	}
}

func getHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"key": args[1],
	}
	resp := IDBHandle.HustdbGet(params)
	if resp.Code == 200 {
		return &Result{
			status: successStatus,
			data:   resp.Data,
		}
	} else {
		return &Result{
			status: nilStatus,
		}
	}
}

func existsHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"key": args[1],
	}
	result := &Result{
		status: integerStatus,
	}
	resp := IDBHandle.HustdbExist(params)
	if resp.Code == 200 {
		result.integer = 1
	} else {
		result.integer = 0
	}
	return result
}

func delHandle(args [][]byte) *Result {
	var delCnt int
	argc := len(args[1:])
	ch := make(chan int, argc)
	for _, key := range args[1:] {
		params := map[string][]byte{
			"key": key,
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbDel(params)
			ch <- resp.Code
		}(params)
	}

	for i := 0; i < argc; i++ {
		code := <-ch
		if code == 200 {
			delCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: delCnt,
	}
}

func strlenHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"key": args[1],
	}
	result := &Result{
		status: integerStatus,
	}
	resp := IDBHandle.HustdbGet(params)
	if resp.Code == 200 {
		result.integer = len(resp.Data)
	} else {
		result.integer = 0
	}
	return result
}

func hdelHandle(args [][]byte) *Result {
	argc := len(args[2:])
	var delCnt int
	ch := make(chan int, argc)
	for _, key := range args[2:] {
		params := map[string][]byte{
			"tb":  args[1],
			"key": key,
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbHdel(params)
			ch <- resp.Code
		}(params)
	}
	for i := 0; i < argc; i++ {
		if code := <-ch; code == 200 {
			delCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: delCnt,
	}
}

func hexistsHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
	}
	result := &Result{
		status: integerStatus,
	}
	if resp := IDBHandle.HustdbHexist(params); resp.Code == 200 {
		result.integer = 1
	}
	return result
}

func hgetHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
	}
	resp := IDBHandle.HustdbHget(params)
	if resp.Code == 200 {
		return &Result{
			status: successStatus,
			data:   resp.Data,
		}
	} else {
		return &Result{
			status: nilStatus,
		}
	}
}

func hincrbyHandle(args [][]byte) *Result {
	result := &Result{}
	_, err := strconv.ParseInt(utils.BytesToString(args[3]), 10, 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR hash value is not an integer")
		return result
	}
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
		"val": args[3],
	}
	resp := IDBHandle.HustdbHincrby(params)
	if resp.Code == 200 {
		result.status = successStatus
		result.data = resp.Data
	} else {
		result.status = successStatus
		result.data = []byte("0")
	}
	return result
}

func hsetHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
		"val": args[3],
	}
	resp := IDBHandle.HustdbHset(params)
	if resp.Code == 200 && resp.Version == 1 {
		return &Result{
			status:  integerStatus,
			integer: 1,
		}
	} else {
		return &Result{
			status:  integerStatus,
			integer: 0,
		}
	}
}

func hlenHandle(args [][]byte) *Result {
	result := &Result{
		status:  integerStatus,
		integer: 0,
	}
	params := map[string][]byte{
		"tb": args[1],
	}
	resp := IDBHandle.HustdbStat(params)
	if resp.Code == 200 {
		hashSize, err := strconv.Atoi(string(resp.Data))
		if err == nil {
			if hashSize != -1 {
				result.integer = hashSize
			}
		}
	}
	return result
}

func saddHandle(args [][]byte) *Result {
	var addCnt int
	argc := len(args[2:])
	ch := make(chan int, argc)
	for _, key := range args[2:] {
		params := map[string][]byte{
			"tb":  args[1],
			"key": key,
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbSadd(params)
			if resp.Version == 1 {
				ch <- resp.Code
			} else {
				ch <- 404
			}
		}(params)
	}
	for i := 0; i < argc; i++ {
		if code := <-ch; code == 200 {
			addCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: addCnt,
	}
}

func sismemberHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
	}
	resp := IDBHandle.HustdbSismember(params)
	result := &Result{
		status: integerStatus,
	}
	if resp.Code == 200 {
		result.integer = 1
	}
	return result
}

func sremHandle(args [][]byte) *Result {
	var remCnt int
	argc := len(args[2:])
	ch := make(chan int, argc)
	for _, key := range args[2:] {
		params := map[string][]byte{
			"tb":  args[1],
			"key": key,
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbSrem(params)
			ch <- resp.Code
		}(params)
	}
	for i := 0; i < argc; i++ {
		if code := <-ch; code == 200 {
			remCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: remCnt,
	}
}

func zaddHandle(args [][]byte) *Result {
	var addCnt int
	argc := len(args[2:])
	if argc%2 != 0 {
		return &Result{
			status: errStatus,
			data:   []byte("ERR syntax error"),
		}
	}
	ch := make(chan int, argc/2)
	for i := 0; i < argc; i += 2 {
		_, err := strconv.ParseFloat(utils.BytesToString(args[2+i]), 64)
		if err != nil {
			return &Result{
				status: errStatus,
				data:   []byte("ERR value is not a valid float"),
			}
		}
		params := map[string][]byte{
			"tb":    args[1],
			"score": args[2+i],
			"key":   args[3+i],
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbZadd(params)
			if resp.Version == 1 {
				ch <- resp.Code
			} else {
				ch <- 404
			}
		}(params)
	}
	for i := 0; i < argc; i += 2 {
		if code := <-ch; code == 200 {
			addCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: addCnt,
	}
}

func zrangeHandle(args [][]byte) *Result {
	var withscores bool
	var resArray []map[string]interface{}
	argc := len(args)
	result := &Result{
		status: arrayStatus,
	}
	start, err := strconv.ParseInt(utils.BytesToString(args[2]), 10, 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR value is not an integer or out of range")
		return result
	}
	end, err := strconv.ParseInt(utils.BytesToString(args[3]), 10, 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR value is not an integer or out of range")
		return result
	}
	if end < start {
		return result
	}
	params := map[string][]byte{
		"tb":     args[1],
		"offset": args[2],
		"size":   []byte(strconv.FormatInt(end-start+1, 10)),
	}
	if argc == 4 {
		params["noval"] = []byte("true")
		withscores = false
	} else {
		if bytes.Compare([]byte("withscores"), bytes.ToLower(args[4])) != 0 {
			result.status = errStatus
			result.data = []byte("ERR syntax error")
			return result
		}
		params["noval"] = []byte("false")
		withscores = true
	}
	resp := IDBHandle.HustdbZrangebyrank(params)
	if resp.Code == 200 {
		json.Unmarshal(resp.Data, &resArray)
	}
	if withscores {
		result.array = make([]string, 0, 2*len(resArray))
	} else {
		result.array = make([]string, 0, len(resArray))
	}
	for _, item := range resArray {
		key := item["key"].(string)
		b, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			continue
		}
		result.array = append(result.array, utils.BytesToString(b))
		if withscores {
			result.array = append(result.array, item["val"].(string))
		}
	}
	return result
}

func zrangeByScoreHandle(args [][]byte) *Result {
	var withscores bool
	var resArray []map[string]interface{}
	argc := len(args)
	result := &Result{
		status: arrayStatus,
	}

	var start, end float64
	var err error
	var start_open_flag bool = false
	var end_open_flag bool = false
	var min, max string
	if bytes.IndexByte(args[2], '(') == 0 {
		args[2] = args[2][1:]
		start_open_flag = true
	}
	start, err = strconv.ParseFloat(utils.BytesToString(args[2]), 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR min or max is not a float")
		return result
	}
	if bytes.IndexByte(args[3], '(') == 0 {
		args[3] = args[3][1:]
		end_open_flag = true
	}
	end, err = strconv.ParseFloat(utils.BytesToString(args[3]), 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR min or max is not a float")
		return result
	}
	if end < start {
		return result
	}
	if start_open_flag {
		min = fmt.Sprintf("%f", start+1)
	} else {
		min = fmt.Sprintf("%f", start)
	}

	if end_open_flag {
		max = fmt.Sprintf("%f", end-1)
	} else {
		max = fmt.Sprintf("%f", end)
	}

	params := map[string][]byte{
		"tb":  args[1],
		"min": []byte(min),
		"max": []byte(max),
	}
	preprocessWithscores := func() *Result {
		params["noval"] = []byte("false")
		withscores = true
		return nil
	}
	preprocessLimit := func(pos int) *Result {
		_, err1 := strconv.ParseInt(utils.BytesToString(args[pos+1]), 10, 64)
		_, err2 := strconv.ParseInt(utils.BytesToString(args[pos+2]), 10, 64)
		if err1 != nil || err2 != nil {
			result.status = errStatus
			result.data = []byte("ERR value is not an integer or out of range")
			return result
		}
		params["offset"] = args[pos+1]
		params["size"] = args[pos+2]

		return nil
	}

	for i := 4; i < argc; i++ {
		arg := bytes.ToLower(args[i])
		if bytes.Compare(arg, []byte("withscores")) == 0 {
			if result := preprocessWithscores(); result != nil {
				return result
			}
		} else if bytes.Compare(arg, []byte("limit")) == 0 {
			//At least limit have two arguments
			if i >= argc-2 {
				return &Result{
					status: errStatus,
					data:   []byte("ERR syntax error"),
				}
			} else {
				if result := preprocessLimit(i); result != nil {
					return result
				}
				i += 2
			}
		} else {
			return &Result{
				status: errStatus,
				data:   []byte("ERR syntax error"),
			}
		}
	}

	resp := IDBHandle.HustdbZrangebyscore(params)
	if resp.Code == 200 {
		json.Unmarshal(resp.Data, &resArray)
	}
	if withscores {
		result.array = make([]string, 0, 2*len(resArray))
	} else {
		result.array = make([]string, 0, len(resArray))
	}
	for _, item := range resArray {
		key := item["key"].(string)
		b, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			continue
		}
		result.array = append(result.array, utils.BytesToString(b))
		if withscores {
			result.array = append(result.array, item["val"].(string))
		}
	}
	return result
}

func zremHandle(args [][]byte) *Result {
	var remCnt int
	argc := len(args[2:])
	ch := make(chan int, argc)
	for _, key := range args[2:] {
		params := map[string][]byte{
			"tb":  args[1],
			"key": key,
		}
		go func(params map[string][]byte) {
			resp := IDBHandle.HustdbZrem(params)
			ch <- resp.Code
		}(params)
	}
	for i := 0; i < argc; i++ {
		if code := <-ch; code == 200 {
			remCnt++
		}
	}
	return &Result{
		status:  integerStatus,
		integer: remCnt,
	}
}

func zscoreHandle(args [][]byte) *Result {
	params := map[string][]byte{
		"tb":  args[1],
		"key": args[2],
	}
	resp := IDBHandle.HustdbZscore(params)
	if resp.Code == 200 {
		return &Result{
			status: successStatus,
			data:   resp.Data,
		}
	} else {
		return &Result{
			status: nilStatus,
		}
	}
}

func zincrbyHandle(args [][]byte) *Result {
	result := &Result{}
	opt := "1"
	if bytes.IndexByte(args[2], '-') == 0 {
		args[2] = args[2][1:]
		opt = "-1"
	}
	_, err := strconv.ParseFloat(utils.BytesToString(args[2]), 64)
	if err != nil {
		result.status = errStatus
		result.data = []byte("ERR hash value is not an double")
		return result
	}
	params := map[string][]byte{
		"tb":    args[1],
		"score": args[2],
		"key":   args[3],
		"opt":   []byte(opt),
	}
	resp := IDBHandle.HustdbZadd(params)
	if resp.Code == 200 {
		result.status = successStatus
		result.data = resp.Data
	} else {
		result.status = successStatus
		result.data = []byte("0")
	}
	return result
}

/*
func rpushHandle(args [][]byte) *Result {
	result := &Result{}
	params := map[string][]byte{
		"queue": args[1],
	}
	for i := 2; i < len(args); i++ {
		params["item"] = args[i]
		IDBHandle.HustmqPut(params)
	}
	return result
}

func lpopHandle(args [][]byte) *Result {
	result := &Result{}
	params := map[string][]byte{
		"queue":  args[1],
		"worker": []byte("worker"),
	}
	resp := IDBHandle.HustmqGet(params)
	if resp.Code == 200 {
		result.status = successStatus
		result.data = resp.Data
	} else {
		result.status = nilStatus
	}
	return result
}

func llenHandle(args [][]byte) *Result {
	result := &Result{
		status:  integerStatus,
		integer: 0,
	}

	mqInfo := map[string]interface{}{}
	params := map[string][]byte{
		"queue": args[1],
	}
	resp := IDBHandle.HustmqStat(params)
	if resp.Code == 200 {
		if err := json.Unmarshal(resp.Data, &mqInfo); err == nil {
			if _, ok := mqInfo["ready"]; ok {
				if ready, ok := mqInfo["ready"].([]interface{}); ok {
					for i := 0; i < len(ready); i++ {
						if val, ok := ready[i].(float64); ok {
							result.integer += int(val)
						}
					}
				}
			}
		}
	}
	return result
}
*/

func echoHandle(args [][]byte) *Result {
	return &Result{
		status: successStatus,
		data:   args[1],
	}
}

func pingHandle(args [][]byte) *Result {
	return &Result{
		status: successStatus,
		data:   []byte("PONG"),
	}
}
