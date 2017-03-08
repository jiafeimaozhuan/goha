package peers

import "../../internal/utils"

func FetchHustdbMaster(key string) string {
	index := utils.LocateHashRegion(key)
	backendInfo := (*globalhashtable)[index]
	if backendInfo.Master.Alive {
		return backendInfo.Master.Host
	}

	return ""
}

func FetchHustdbSlaver(key string) string {
	index := utils.LocateHashRegion(key)
	backendInfo := (*globalhashtable)[index]
	if backendInfo.Slave.Alive {
		return backendInfo.Slave.Host
	}

	return ""
}

func FetchHustdbPeers(key string) []string {
	index := utils.LocateHashRegion(key)
	backendInfo := (*globalhashtable)[index]

	backends := make([]string, 0, 2)
	if backendInfo.Master.Alive {
		backends = append(backends, backendInfo.Master.Host)
	}
	if backendInfo.Slave.Alive {
		backends = append(backends, backendInfo.Slave.Host)
	}

	return backends
}

func FetchHustdbHincrbyPeers(key string) []string {
	index := utils.LocateHashRegion(key)
	backendInfo := (*globalhashtable)[index]
	if backendInfo.Master.Alive {
		return []string{backendInfo.Master.Host, backendInfo.Slave.Host}
	}
	if backendInfo.Slave.Alive {
		return []string{backendInfo.Slave.Host, backendInfo.Master.Host}
	}

	return nil
}

func FetchHustdbStatPeers() []string {
	HaTable.Rwlock.RLock()
	defer HaTable.Rwlock.RUnlock()

	peers := []string{}
	peerSet := map[string]bool{}
	masterAlive := false
	for _, peer := range HaTable.HashTable {
		if peer.Backends.Master.Alive {
			if _, ok := peerSet[peer.Backends.Master.Host]; !ok {
				peerSet[peer.Backends.Master.Host] = true
				peers = append(peers, peer.Backends.Master.Host)
			}
			masterAlive = true
		}

		if peer.Backends.Slave.Alive {
			if _, ok := peerSet[peer.Backends.Slave.Host]; !ok {
				peerSet[peer.Backends.Slave.Host] = true
				if !masterAlive {
					peers = append(peers, peer.Backends.Slave.Host)
				}
			}
			continue
		}

		if !masterAlive {
			return nil
		}
	}

	return peers
}
