package leader

import (
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sort"
	"sync"
	"time"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	replicasNeedList []gfs.ChunkHandle // list of handles need a new replicas
	// Happens when some servers are disconnected
	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex

	replicaAddresses []gfs.ServerAddress // set of replica replicaAddresses
	primary          gfs.ServerAddress   // primary chunk-server
	expire           time.Time           // lease expire time
	version          gfs.ChunkVersion
	checksum         gfs.Checksum
	path             gfs.Path
}

type fileInfo struct {
	sync.RWMutex
	handles []gfs.ChunkHandle
}

type serialChunkInfo struct {
	Path gfs.Path
	Info []gfs.PersistentChunkInfo
}

func (cm *chunkManager) Deserialize(files []serialChunkInfo) error {
	cm.Lock()
	defer cm.Unlock()

	now := time.Now()
	for _, v := range files {
		log.Info("Master restore files ", v.Path)
		f := new(fileInfo)
		for _, ck := range v.Info {
			f.handles = append(f.handles, ck.Handle)
			log.Info("Master restore chunk ", ck.Handle)
			cm.chunk[ck.Handle] = &chunkInfo{
				expire:   now,
				version:  ck.Version,
				checksum: ck.Checksum,
			}
		}
		cm.numChunkHandle += gfs.ChunkHandle(len(v.Info))
		cm.file[v.Path] = f
	}

	return nil
}

func (cm *chunkManager) Serialize() []serialChunkInfo {
	cm.RLock()
	defer cm.RUnlock()

	var ret []serialChunkInfo
	for k, v := range cm.file {
		var chunks []gfs.PersistentChunkInfo
		for _, handle := range v.handles {
			chunks = append(chunks, gfs.PersistentChunkInfo{
				Handle:   handle,
				Length:   0,
				Version:  cm.chunk[handle].version,
				Checksum: 0,
			})
		}

		ret = append(ret, serialChunkInfo{Path: k, Info: chunks})
	}

	return ret
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	log.Info("----------------------")
	log.Info("New chunk manager!")
	log.Info("----------------------")
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress, useLock bool) error {
	var ck *chunkInfo
	var ok bool

	if useLock {
		cm.RLock()
		ck, ok = cm.chunk[handle]
		cm.RUnlock()

		// since we are appending the chunk replicaAddresses with replica address. and this block guarantees locking.
		ck.Lock()
		defer ck.Unlock()
	} else {
		ck, ok = cm.chunk[handle]
	}

	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	ck.replicaAddresses = append(ck.replicaAddresses, addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cannot find chunk %v", handle)
	}
	return ck.replicaAddresses, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	cm.RUnlock()

	filePathInfo, ok := cm.file[path]
	if !ok {
		return -1, fmt.Errorf("cannot get handle for %v[%v]", path, index)
	}

	if index < 0 || int(index) >= len(filePathInfo.handles) {
		return -1, fmt.Errorf("Invalid index for %v[%v]", path, index)
	}

	return filePathInfo.handles[index], nil
}

// GetLeaseHolder returns the chunk-server that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, []gfs.ServerAddress, error) {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("invalid chunk handle %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	var staleServers []gfs.ServerAddress

	ret := &gfs.Lease{}
	// if expiration is done.
	if ck.expire.Before(time.Now()) { // grants a new lease
		// check version
		ck.version++
		arg := gfs.CheckVersionArg{Handle: handle, Version: ck.version}

		var newChunkReplicasNeedList []string
		var lock sync.Mutex // lock for newChunkReplicasNeedList

		var wg sync.WaitGroup

		// How many times it needs to wait.
		wg.Add(len(ck.replicaAddresses))
		for _, v := range ck.replicaAddresses {
			go func(addr gfs.ServerAddress) {
				var r gfs.CheckVersionResponse

				// TODO distinguish call error and r.Stale
				err := util.Execute(addr, "ChunkServer.RPCCheckVersion", arg, &r)
				if err == nil && r.Stale == false {
					lock.Lock()
					newChunkReplicasNeedList = append(newChunkReplicasNeedList, string(addr))
					lock.Unlock()
				} else { // add to garbage collection
					log.Warningf("detect stale chunk %v in %v (err: %v)", handle, addr, err)
					staleServers = append(staleServers, addr)
				}
				// Mark one instance of replica done.
				wg.Done()
			}(v)
		}
		wg.Wait()

		ck.replicaAddresses = make([]gfs.ServerAddress, len(newChunkReplicasNeedList))
		for i := range newChunkReplicasNeedList {
			ck.replicaAddresses[i] = gfs.ServerAddress(newChunkReplicasNeedList[i])
		}
		log.Warning(handle, " lease location ", ck.replicaAddresses)

		if len(ck.replicaAddresses) < gfs.MinimumNumReplicas {
			cm.Lock()
			cm.replicasNeedList = append(cm.replicasNeedList, handle)
			cm.Unlock()

			if len(ck.replicaAddresses) == 0 {
				// !! ATTENTION !!
				ck.version--
				return nil, nil, fmt.Errorf("no replica of %v", handle)
			}
		}

		// TODO choose primary, !!error handle no replicas!!
		ck.primary = ck.replicaAddresses[0]
		ck.expire = time.Now().Add(gfs.LeaseExpire)
	}

	ret.Primary = ck.primary
	ret.Expire = ck.expire
	for _, v := range ck.replicaAddresses {
		if v != ck.primary {
			ret.Secondaries = append(ret.Secondaries, v)
		}
	}
	return ret, staleServers, nil
}

// ExtendLease extends the lease of chunk if the leaseholder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	return nil
	//log.Fatal("unsupported ExtendLease")
	//cm.RLock()
	//ck, ok := cm.chunk[handle]
	//cm.RUnlock()
	//
	//ck.Lock()
	//defer ck.Unlock()
	//
	//if !ok {
	//	return fmt.Errorf("invalid chunk handle %v", handle)
	//}
	//
	//now := time.Now()
	//if ck.primary != primary && ck.expire.After(now) {
	//	return fmt.Errorf("%v does not hold the lease for chunk %v", primary, handle)
	//}
	//ck.primary = primary
	//ck.expire = now.Add(gfs.LeaseExpire)
	//return nil
}

// CreateChunk creates a new chunk for path. servers for the chunk are denoted by addrs
// returns the handle of the new chunk, and the servers that create the chunk successfully
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, []gfs.ServerAddress, error) {
	cm.Lock()
	defer cm.Unlock()

	handle := cm.numChunkHandle
	cm.numChunkHandle++

	// update file info
	filePathInfo, ok := cm.file[path]
	if !ok {
		filePathInfo = new(fileInfo)
		cm.file[path] = filePathInfo
	}
	filePathInfo.handles = append(filePathInfo.handles, handle)

	// update chunk info
	ck := &chunkInfo{path: path}
	cm.chunk[handle] = ck

	var errList string
	var success []gfs.ServerAddress
	for _, v := range addrs {
		var r gfs.CreateChunkResponse

		err := util.Execute(v, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &r)
		if err == nil { // register
			ck.replicaAddresses = append(ck.replicaAddresses, v)
			success = append(success, v)
		} else {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return handle, success, nil
	} else {
		// replicas are no enough, add to need list
		cm.replicasNeedList = append(cm.replicasNeedList, handle)
		return handle, success, fmt.Errorf(errList)
	}
}

// RemoveChunks removes disconnected chunks
// if replicas number of a chunk is less than gfs.MinimumNumReplicas, add it to chunk-replica-need list
func (cm *chunkManager) RemoveChunks(handles []gfs.ChunkHandle, server gfs.ServerAddress) error {

	errList := ""
	for _, v := range handles {
		cm.RLock()
		ck, ok := cm.chunk[v]
		cm.RUnlock()

		if !ok {
			log.Errorf("ERROR occurred when removing chunk handle!")
			continue
		}

		ck.Lock()
		var newChunksThatNeedReplicas []gfs.ServerAddress
		for i := range ck.replicaAddresses {
			if ck.replicaAddresses[i] != server {
				newChunksThatNeedReplicas = append(newChunksThatNeedReplicas, ck.replicaAddresses[i])
			}
		}
		ck.replicaAddresses = newChunksThatNeedReplicas
		ck.expire = time.Now()
		num := len(ck.replicaAddresses)
		ck.Unlock()

		if num < gfs.MinimumNumReplicas {
			cm.replicasNeedList = append(cm.replicasNeedList, v)
			if num == 0 {
				log.Error("lost all replicas of %v", v)
				errList += fmt.Sprintf("Lose all replicas of chunk %v;", v)
			}
		}
	}

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// GetReplicasNeedlist - clears the need list at first (removes the old handles that no longer need replicas)
// This happens if a replica is crashed or misses its heartbeat.
// and then return all new handles
func (cm *chunkManager) GetReplicasNeedlist() []gfs.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()

	// clear satisfied chunk
	var newChunksThatNeedReplicas []int
	for _, v := range cm.replicasNeedList {
		if len(cm.chunk[v].replicaAddresses) < gfs.MinimumNumReplicas {
			newChunksThatNeedReplicas = append(newChunksThatNeedReplicas, int(v))
		}
	}

	// make unique
	sort.Ints(newChunksThatNeedReplicas)
	cm.replicasNeedList = make([]gfs.ChunkHandle, 0)
	for i, v := range newChunksThatNeedReplicas {
		if i == 0 || v != newChunksThatNeedReplicas[i-1] {
			cm.replicasNeedList = append(cm.replicasNeedList, gfs.ChunkHandle(v))
		}
	}

	if len(cm.replicasNeedList) > 0 {
		return cm.replicasNeedList
	} else {
		return nil
	}
}
