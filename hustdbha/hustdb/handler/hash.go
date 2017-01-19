package handler

import (
	binlog "hustdbha/hustdb/binlog"
	"hustdbha/hustdb/comm"
	"hustdbha/hustdb/peers"
)

func (p *HustdbHandler) HustdbHget(args map[string][]byte) *comm.HustdbResponse {
	ikey, ok := args["key"]
	key := string(ikey)
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(key)
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbHget(backend, args, retChan)
	}

	maxVer := 0
	hustdbResp := &comm.HustdbResponse{Code: comm.HttpNotFound}
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		if resp.Code == comm.HttpOk && resp.Version > maxVer {
			hustdbResp.Code = comm.HttpOk
			maxVer = resp.Version
			hustdbResp.Data = resp.Data
		}
	}

	return hustdbResp
}

func (p *HustdbHandler) HustdbHset(args map[string][]byte) *comm.HustdbResponse {
	ikey, ok := args["key"]
	key := string(ikey)
	if !ok {
		return NilHustdbResponse
	}
	val, ok := args["val"]
	if !ok {
		return NilHustdbResponse
	}
	delete(args, "val")

	backends := peers.FetchHustdbPeers(key)
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbHset(backend, args, val, retChan)
	}

	putSucc := 0
	var putFailedBackend string
	var putSuccessBackend string
	hustdbResp := &comm.HustdbResponse{Code: 0}
	for ix := 0; ix < cap(retChan); ix++ {
		select {
		case resp := <-retChan:
			if resp.Code == comm.HttpOk {
				putSucc++
				hustdbResp.Code = comm.HttpOk
				putSuccessBackend = resp.Backend
			} else {
				putFailedBackend = resp.Backend
			}
		}
	}

	/* Need Binlog */
	if putSucc != 0 && putSucc != len(backends) {
		binlog.Do(putSuccessBackend, putFailedBackend, "hset", args, val)
	}

	return hustdbResp
}

func (p *HustdbHandler) HustdbHexist(args map[string][]byte) *comm.HustdbResponse {
	ikey, ok := args["key"]
	key := string(ikey)
	if !ok {
		return NilHustdbResponse
	}
	backends := peers.FetchHustdbPeers(key)
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbHexist(backend, args, retChan)
	}

	hustdbResp := &comm.HustdbResponse{Code: comm.HttpNotFound}
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		if resp.Code == comm.HttpOk {
			hustdbResp.Code = comm.HttpOk
		}
	}

	return hustdbResp
}

func (p *HustdbHandler) HustdbHdel(args map[string][]byte) *comm.HustdbResponse {
	ikey, ok := args["key"]
	key := string(ikey)
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(key)
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbDel(backend, args, retChan)
	}

	delSucc := 0
	var delFailedBackend string
	var delSuccessBackend string
	hustdbResp := &comm.HustdbResponse{Code: 0}
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		if resp.Code == comm.HttpOk {
			delSucc++
			hustdbResp.Code = comm.HttpOk
			delSuccessBackend = resp.Backend
		} else {
			delFailedBackend = resp.Backend
		}
	}

	/* Need Binlog */
	if delSucc != 0 && delSucc != len(backends) {
		binlog.Do(delSuccessBackend, delFailedBackend, "hdel", args, nil)
	}
	return hustdbResp
}

func (p *HustdbHandler) HustdbHincrby(args map[string][]byte) *comm.HustdbResponse {
	ikey, ok := args["key"]
	key := string(ikey)
	if !ok {
		return NilHustdbResponse
	}

	peers := peers.FetchHustdbHincrbyPeers(key)
	if len(peers) == 0 || len(peers) != 2 {
		return NilHustdbResponse
	}

	args["host"] = []byte(peers[1])
	return comm.HustdbHincrby(peers[0], args)
}
