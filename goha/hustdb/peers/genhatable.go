package peers

import (
	"goha/hustdb/comm"
	"goha/internal/utils"
	"sync"

	"github.com/cihub/seelog"
)

type HaTableStruct struct {
	HashTable []*PeerInfo
	Rwlock    sync.RWMutex
}

type PeerInfo struct {
	Region   []int        `json:"region,omitempty"`
	Backends *BackendInfo `json:"backends,omitempty"`
}

type BackendDetail struct {
	Host  string `json:"host,omitempty"`
	Alive bool   `json:alive,omitempty`
}

type BackendInfo struct {
	Master BackendDetail `json:"master,omitempty"`
	Slave  BackendDetail `json:"slave,omitempty"`
}

var HaTable *HaTableStruct

type GlobalHashTable *[]BackendInfo

var globalhashtable GlobalHashTable

func Init(path string) bool {
	if !LoadHashTable(path) {
		return false
	}
	return RefreshGlobleHashtable()
}

func LoadHashTable(path string) bool {
	HaTable = new(HaTableStruct)
	HaTable.Rwlock = sync.RWMutex{}
	return utils.LoadConf(path, &HaTable.HashTable)
}

func RefreshHashTable(path string) bool {
	HaTable.Rwlock.Lock()
	rc := utils.LoadConf(path, &HaTable.HashTable)
	HaTable.Rwlock.Unlock()
	return rc
}

func GenGlobleHashtable() bool {
	ghTable := make([]BackendInfo, comm.HustdbTableSize)
	HaTable.Rwlock.RLock()
	for _, peer := range HaTable.HashTable {
		if len(peer.Region) != 2 {
			seelog.Critical("Globalhashtable Format Error")
			return false
		}
		for ix := peer.Region[0]; ix < peer.Region[1]; ix++ {
			ghTable[ix] = *peer.Backends
		}
	}

	globalhashtable = &ghTable
	HaTable.Rwlock.RUnlock()
	return true
}

func RefreshGlobleHashtable() bool {
	HaTable.Rwlock.RLock()
	for _, peer := range HaTable.HashTable {
		if len(peer.Region) != 2 {
			seelog.Critical("Globalhashtable Format Error")
			return false
		}
		for ix := peer.Region[0]; ix < peer.Region[1]; ix++ {
			(*globalhashtable)[ix] = *peer.Backends
		}
	}

	HaTable.Rwlock.RUnlock()
	return true
}

func SaveHashTable(path string) bool {
	return utils.SaveConf(HaTable.HashTable, path)
}

func GetGlobleHashtable() *GlobalHashTable {
	return &globalhashtable
}
