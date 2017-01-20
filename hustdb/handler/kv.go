package handler

import (
	binlog "goha/hustdb/binlog"
	"goha/hustdb/comm"
	"goha/hustdb/peers"
	"time"

	"github.com/cihub/seelog"
)

func (p *HustdbHandler) HustdbGet2(args map[string][]byte) *comm.HustdbResponse {
	startTs := time.Now()
	key, ok := args["key"]
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(string(key))
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbGet2(backend, args, retChan)
	}

	maxVer := 0
	hustdbResp := &comm.HustdbResponse{Code: 0}
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		//fmt.Printf("Resp : %v\n", resp)
		if resp.Code == comm.HttpOk && resp.Version > maxVer {
			hustdbResp.Code = comm.HttpOk
			maxVer = resp.Version
			hustdbResp.Data = resp.Data
		}
	}

	seelog.Debugf("Get Time Elapsed : %v", time.Since(startTs))

	return hustdbResp
}

func (p *HustdbHandler) HustdbGet(args map[string][]byte) *comm.HustdbResponse {
	key, ok := args["key"]
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(string(key))

	hustdbResp := &comm.HustdbResponse{Code: 0}
	for _, backend := range backends {
		resp := comm.HustdbGet(backend, args)
		if resp.Code == comm.HttpOk {
			return resp
		}
	}

	return hustdbResp
}

func (p *HustdbHandler) HustdbPut(args map[string][]byte) *comm.HustdbResponse {
	startTs := time.Now()
	key, ok := args["key"]
	if !ok {
		return NilHustdbResponse
	}
	val, ok := args["val"]
	if !ok {
		return NilHustdbResponse
	}
	delete(args, "val")

	backends := peers.FetchHustdbPeers(string(key))
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbPut(backend, args, val, retChan)
	}

	putSucc := 0
	var putFailedBackend string
	var putSuccessBackend string
	hustdbResp := &comm.HustdbResponse{Code: 0}
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		if resp.Code == comm.HttpOk {
			putSucc++
			hustdbResp.Code = comm.HttpOk
			putSuccessBackend = resp.Backend
		} else {
			putFailedBackend = resp.Backend
		}
	}

	/* Need Binlog */
	if putSucc == 1 && putSucc != len(backends) {
		delete(args, "key")
		binlog.Do(putSuccessBackend, putFailedBackend, "put", args, key)
	}

	seelog.Debugf("Put Time Elapsed : %v", time.Since(startTs))
	return hustdbResp
}

func (p *HustdbHandler) HustdbExist(args map[string][]byte) *comm.HustdbResponse {
	key, ok := args["key"]
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(string(key))
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbExist(backend, args, retChan)
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

func (p *HustdbHandler) HustdbDel(args map[string][]byte) *comm.HustdbResponse {
	key, ok := args["key"]
	if !ok {
		return NilHustdbResponse
	}

	backends := peers.FetchHustdbPeers(string(key))
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
	if delSucc == 1 && delSucc != len(backends) {
		delete(args, "key")
		binlog.Do(delSuccessBackend, delFailedBackend, "del", args, key)
	}
	return hustdbResp
}
