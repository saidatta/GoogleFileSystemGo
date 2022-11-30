package leader

import (
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// chunkServerManager manages chunk-servers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunk-server has
	garbage       []gfs.ChunkHandle
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	log.Info("-----------new chunk server manager")
	return csm
}

func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress, response *gfs.HeartbeatResponse) bool {
	csm.Lock()
	defer csm.Unlock()

	serverInfo, ok := csm.servers[addr]
	if !ok {
		log.Info("New chunk server - " + addr)
		csm.servers[addr] = &chunkServerInfo{time.Now(), make(map[gfs.ChunkHandle]bool), nil}
		return true
	} else {
		// send garbage
		response.Garbage = csm.servers[addr].garbage
		csm.servers[addr].garbage = make([]gfs.ChunkHandle, 0)
		serverInfo.lastHeartbeat = time.Now()
		return false
	}
}

// AddChunk - register a chunk to servers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	for _, v := range addrs {
		//csm.servers[v].chunks[handle] = true
		sv, ok := csm.servers[v]
		if ok {
			sv.chunks[handle] = true
		} else {
			log.Warning("add chunk in removed server ", sv)
		}
	}
}

// AddGarbage - mark addresses that need to be garbage collected.
func (csm *chunkServerManager) AddGarbage(addr gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if ok {
		sv.garbage = append(sv.garbage, handle)
	}
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
// re-replication is also used for other usecases, bsased on chunk distribution and chunk server disk space. That logic is TODO
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil
	for a, v := range csm.servers {
		// naive logic to find to server for the handle replicaiton.
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("no appropriate replica found for this chunk -  %v", handle)
	return
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is created.
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {

	if num > len(csm.servers) {
		return nil, fmt.Errorf("not enough servers for %v replicas", num)
	}

	csm.RLock()
	var allServerAddresses, ret []gfs.ServerAddress
	for a, _ := range csm.servers {
		allServerAddresses = append(allServerAddresses, a)
	}
	csm.RUnlock()

	choose, err := util.FindRandomKServers(len(allServerAddresses), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, allServerAddresses[v])
	}

	return ret, nil
}

// DetectDeadServers detect disconnected servers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	var ret []gfs.ServerAddress
	now := time.Now()
	for k, v := range csm.servers {
		if v.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) {
			ret = append(ret, k)
		}
	}

	return ret
}

// RemoveServer RemoveServers removes metadata of disconnected server
// it returns the chunks that server holds.
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	err = nil
	sv, ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("cannot find chunk server - %v", addr)
		return
	}
	for h, v := range sv.chunks {
		if v {
			handles = append(handles, h)
		}
	}
	delete(csm.servers, addr)

	return
}
