package healthcheck

import (
	"goha/hustdb/comm"
	"goha/hustdb/peers"
	"time"

	"github.com/cihub/seelog"
)

var (
	HealthCheckCycle time.Duration
)

func Init(cycle int) {
	HealthCheckCycle = time.Duration(cycle)
}

type PeerStatusInfo struct {
	Idx   int
	Role  string
	Host  string
	Alive bool
}

func RefreshGlobalHaTable(peer *PeerStatusInfo, status bool) bool {
	peers.HaTable.Rwlock.Lock()
	switch peer.Role {
	case "master":
		peers.HaTable.HashTable[peer.Idx].Backends.Master.Alive = status
	case "slave":
		peers.HaTable.HashTable[peer.Idx].Backends.Slave.Alive = status
	default:
		break
	}

	peers.HaTable.Rwlock.Unlock()
	return true
}

func CheckOnce() {
	peers.HaTable.Rwlock.RLock()
	retChan := make(chan bool, 2*len(peers.HaTable.HashTable))
	for idx, item := range peers.HaTable.HashTable {
		masterPeerInfo := &PeerStatusInfo{
			Idx:   idx,
			Role:  "master",
			Host:  item.Backends.Master.Host,
			Alive: item.Backends.Master.Alive,
		}
		go IsAlive(masterPeerInfo, retChan, RefreshGlobalHaTable)

		slavePeerInfo := &PeerStatusInfo{
			Idx:   idx,
			Role:  "slave",
			Host:  item.Backends.Slave.Host,
			Alive: item.Backends.Slave.Alive,
		}
		go IsAlive(slavePeerInfo, retChan, RefreshGlobalHaTable)
	}

	peers.HaTable.Rwlock.RUnlock()
	needAdjust := false
	for ix := 0; ix < cap(retChan); ix++ {
		select {
		case ret := <-retChan:
			if ret == true {
				needAdjust = true
			}
			break
		case <-time.After(time.Second * 5):
			seelog.Warn("CheckOnce Over 5 Second !!!")
			break
		}
	}

	if needAdjust {
		peers.RefreshGlobleHashtable()
	}
}

func HealthCheckLoop() {
	ticker := time.NewTicker(time.Second * HealthCheckCycle)
	go func() {
		for _ = range ticker.C {
			CheckOnce()
		}
	}()
}

func IsAlive(peer *PeerStatusInfo, retChan chan bool, callback func(peer *PeerStatusInfo, status bool) bool) {
	code := comm.HustdbAlive(peer.Host)
	if code != comm.HttpOk && peer.Alive {
		retChan <- callback(peer, false)
	} else if code == comm.HttpOk && !peer.Alive {
		retChan <- callback(peer, true)
	} else {
		retChan <- false
	}
}
