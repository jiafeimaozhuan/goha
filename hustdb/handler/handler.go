package handler

import (
	"strconv"

	"../peers"

	"../comm"
)

type HustdbHandler struct {
}

func NewHustdbHandler() *HustdbHandler {
	return &HustdbHandler{}
}

var NilHustdbResponse = &comm.HustdbResponse{Code: 0}

func (p *HustdbHandler) HustdbStat(args map[string][]byte) *comm.HustdbResponse {
	backends := peers.FetchHustdbStatPeers()
	if len(backends) == 0 {
		return NilHustdbResponse
	}

	retChan := make(chan *comm.HustdbResponse, len(backends))
	for _, backend := range backends {
		go comm.HustdbStat(backend, args, retChan)
	}

	hustdbResp := &comm.HustdbResponse{Code: 0}
	totalCnt := int64(0)
	for ix := 0; ix < cap(retChan); ix++ {
		resp := <-retChan
		if resp.Code != comm.HttpOk {
			return hustdbResp
		}
		cnt, _ := strconv.ParseInt(string(resp.Data), 10, 64)
		totalCnt += cnt
	}

	return &comm.HustdbResponse{Code: comm.HttpOk, Data: []byte(strconv.FormatInt(totalCnt, 10))}
}
