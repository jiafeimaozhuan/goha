package handler

import "hustdbha/hustdb/comm"

type HustdbHandler struct {
}

func NewHustdbHandler() *HustdbHandler {
	return &HustdbHandler{}
}

var NilHustdbResponse = &comm.HustdbResponse{Code: 0}
