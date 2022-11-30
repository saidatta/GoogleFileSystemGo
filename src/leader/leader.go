package leader

import (
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"encoding/gob"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"
)

// Leader Server struct
type Leader struct {
	address        gfs.ServerAddress // leader server address
	serverRootPath string
	l              net.Listener
	shutdown       chan struct{}
	dead           bool // set to ture if server is shuntdown

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// PersistentBlock The block info thats stored within the disk
type PersistentBlock struct {
	NamespaceTree []serialTreeNode
	ChunkInfo     []serialChunkInfo
}

const (
	MetaFileName = "gfs-leader.meta"
	FilePerm     = 0755
)

// NewAndServe starts a leader and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Leader {
	ldr := &Leader{
		address:        address,
		serverRootPath: serverRoot,
		shutdown:       make(chan struct{}),
	}

	// starting up the rpc server at the leader's address.
	rpcs := rpc.NewServer()
	rpcs.Register(ldr)
	l, e := net.Listen("tcp", string(ldr.address))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	ldr.l = l

	// loading the leader metadata
	ldr.initMetadata()

	// RPC Handler
	go func() {
		for {
			select {
			// if shutdown channel signal is sent. Then, shutdown the leader node.
			case <-ldr.shutdown:
				return
			default:
			}
			// accept a new connection.
			conn, err := ldr.l.Accept()
			if err == nil {
				// spin up a go routine for each recieved connection.
				go func() {
					// serve the received connection.
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !ldr.dead {
					log.Fatal("leader encountered an error: ", err)
				}
			}
		}
	}()

	// Background Task
	// BackgroundActivity does all the background activities
	// server disconnection handle, garbage collection, stale replica detection, etc
	go func() {
		checkTicker := time.Tick(gfs.ServerCheckInterval)
		storeTicker := time.Tick(gfs.LeaderStoreInterval)
		for {
			var err error
			select {
			case <-ldr.shutdown:
				return
			case <-checkTicker:
				err = ldr.serverCheck()
			case <-storeTicker:
				err = ldr.backupMeta()
			}
			if err != nil {
				log.Error("Background error ", err)
			}
		}

	}()

	log.Infof("Leader is running now. address here = %v", address)

	return ldr
}

// InitMetadata initiates meta data
func (ldr *Leader) initMetadata() {
	ldr.nm = newNamespaceManager()
	ldr.cm = newChunkManager()
	ldr.csm = newChunkServerManager()
	ldr.loadMeta()
	return
}

// loadMeta loads metadata from disk
func (ldr *Leader) loadMeta() error {
	filename := path.Join(ldr.serverRootPath, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {
		return err
	}

	ldr.nm.Deserialize(meta.NamespaceTree)
	ldr.cm.Deserialize(meta.ChunkInfo)

	return nil
}

// backupMeta stores metadata to disk
func (ldr *Leader) backupMeta() error {
	filename := path.Join(ldr.serverRootPath, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock

	meta.NamespaceTree = ldr.nm.Serialize()
	meta.ChunkInfo = ldr.cm.Serialize()

	log.Infof("Leader : store metadata")
	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// Shutdown shuts down master
func (ldr *Leader) Shutdown() {
	if !ldr.dead {
		log.Warning(ldr.address, "is being Shutdown gracefully!")
		ldr.dead = true
		close(ldr.shutdown)
		ldr.l.Close()
	}

	err := ldr.backupMeta()
	if err != nil {
		log.Warning("error in backing up metadata: ", err)
	}
}

// serverCheck checks all chunk-server according to last heartbeat time
// then removes all the information of the disconnected servers
func (ldr *Leader) serverCheck() error {
	// detect dead servers
	deadServerAddrs := ldr.csm.DetectDeadServers()
	for _, v := range deadServerAddrs {
		log.Warningf("remove server %v", v)
		handles, err := ldr.csm.RemoveServer(v)
		if err != nil {
			return err
		}
		err = ldr.cm.RemoveChunks(handles, v)
		if err != nil {
			return err
		}
	}

	// add replicas for need request
	handles := ldr.cm.GetReplicasNeedlist()
	if handles != nil {
		log.Info("Leader Needs these handles - ", handles)
		ldr.cm.RLock()
		for i := 0; i < len(handles); i++ {
			ck := ldr.cm.chunk[handles[i]]

			// if chunk has expired, and its expirationTime is later than currentTime.
			if ck.expire.Before(time.Now()) {
				ck.Lock() // don't grant lease during copy
				err := ldr.reReplication(handles[i])
				log.Info(err)
				ck.Unlock()
			}
		}
		ldr.cm.RUnlock()
	}
	return nil
}

// reReplication performs re-replication, chunk should be locked in top caller
// new lease will not be granted during copy
func (ldr *Leader) reReplication(handle gfs.ChunkHandle) error {
	// chunk are locked, so master will not grant lease during copy time
	from, to, err := ldr.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	log.Warningf("allocate new chunk %v from %v to %v", handle, from, to)

	// create a new chunk file
	var cr gfs.CreateChunkResponse
	err = util.Execute(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &cr)
	if err != nil {
		return err
	}

	// replicate that that created chunk file
	var sr gfs.SendCopyResponse
	err = util.Execute(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &sr)
	if err != nil {
		return err
	}

	ldr.cm.RegisterReplica(handle, to, false)
	ldr.csm.AddChunk([]gfs.ServerAddress{to}, handle)
	return nil
}

// RPCHeartbeat is called by chunk-server to let the leader know that a chunk-server is alive
func (ldr *Leader) RPCHeartbeat(args gfs.HeartbeatArg, response *gfs.HeartbeatResponse) error {
	isFirst := ldr.csm.Heartbeat(args.Address, response)

	for _, handle := range args.LeaseExtensions {
		continue
		// ATTENTION !! dead lock
		ldr.cm.ExtendLease(handle, args.Address)
	}

	if isFirst { // if is first heartbeat, let chunk-server report itself
		// what chunks it has, addr, and other chunk info.
		var r gfs.ReportSelfResponse
		err := util.Execute(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
		if err != nil {
			return err
		}

		for _, v := range r.Chunks {
			ldr.cm.RLock()
			ck, ok := ldr.cm.chunk[v.Handle]
			if !ok {
				continue
			}
			version := ck.version
			ldr.cm.RUnlock()

			if v.Version == version {
				log.Infof("Leader received chunk - %v from - %v", v.Handle, args.Address)
				ldr.cm.RegisterReplica(v.Handle, args.Address, true)
				ldr.csm.AddChunk([]gfs.ServerAddress{args.Address}, v.Handle)
			} else {
				log.Infof("Leader discarded this chunk - %v", v.Handle)
			}
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns leaseholder and its secondaries for a chunk.
// If no one holds the lease currently, grant one.
// Leader will communicate with all replicas holder to check version, if stale replica is detected,
//it will be added it to garbage collection
func (ldr *Leader) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, response *gfs.GetPrimaryAndSecondariesResponse) error {
	lease, staleServers, err := ldr.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}

	for _, v := range staleServers {
		ldr.csm.AddGarbage(v, args.Handle)
	}

	response.Primary = lease.Primary
	response.Expire = lease.Expire
	response.Secondaries = lease.Secondaries

	// no error.
	return nil
}

// RPCExtendLease extends the lease of chunk if the leaser is nobody or requester.
func (ldr *Leader) RPCExtendLease(args gfs.ExtendLeaseArg, reply *gfs.ExtendLeaseResponse) error {
	//t, err := ldr.cm.ExtendLease(args.Handle, args.Address)
	//if err != nil { return err }
	//reply.Expire = *t
	return nil
}

// RPCGetReplicas is called by client to find all chunk-server that holds the chunk.
func (ldr *Leader) RPCGetReplicas(args gfs.GetReplicasArg, response *gfs.GetReplicasResponse) error {
	servers, err := ldr.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	for _, v := range servers {
		response.Locations = append(response.Locations, v)
	}
	return nil
}

// RPCCreateFile is called by client to create a new file
func (ldr *Leader) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileResponse) error {
	err := ldr.nm.Create(args.Path)
	return err
}

// RPCDeleteFile is called by client to delete a file
func (ldr *Leader) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileResponse) error {
	err := ldr.nm.Delete(args.Path)
	return err
}

// RPCRenameFile is called by client to rename a file
func (ldr *Leader) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileResponse) error {
	err := ldr.nm.Rename(args.SourceName, args.TargetName)
	return err
}

// RPCMkdir is called by client to make a new directory
func (ldr *Leader) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirResponse) error {
	err := ldr.nm.Mkdir(args.Path)
	return err
}

// RPCList is called by client to list all files in specific directory
func (ldr *Leader) RPCList(args gfs.ListArg, reply *gfs.ListResponse) error {
	var err error
	reply.Files, err = ldr.nm.List(args.Path)
	return err
}

// RPCGetFileInfo is called by client to get file information
func (ldr *Leader) RPCGetFileInfo(args gfs.GetFileInfoArg, response *gfs.GetFileInfoResponse) error {
	parents, cwd, err := ldr.nm.lockParents(args.Path, false)
	defer ldr.nm.unlockParents(parents)
	if err != nil {
		return err
	}

	currentParent := parents[len(parents)-1]
	// get children of this parent.
	file, ok := cwd.children[currentParent]
	if !ok {
		return fmt.Errorf("File %v does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	response.IsDir = file.isDir
	response.Length = file.length
	response.TotalChunks = file.chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (ldr *Leader) RPCGetChunkHandle(args gfs.GetChunkHandleArg, response *gfs.GetChunkHandleResponse) error {
	associatedParents, cwd, err := ldr.nm.lockParents(args.Path, false)
	defer ldr.nm.unlockParents(associatedParents)
	if err != nil {
		return err
	}

	// append new chunks
	currentParent := associatedParents[len(associatedParents)-1]
	file, ok := cwd.children[currentParent]
	if !ok {
		return fmt.Errorf("File %v does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	if int(args.Index) == int(file.chunks) {
		file.chunks++

		replicaAddrs, err := ldr.csm.ChooseServers(gfs.DefaultNumReplicas)
		if err != nil {
			return err
		}

		response.Handle, replicaAddrs, err = ldr.cm.CreateChunk(args.Path, replicaAddrs)
		if err != nil {
			log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", response.Handle)
		}

		ldr.csm.AddChunk(replicaAddrs, response.Handle)
	} else {
		response.Handle, err = ldr.cm.GetChunk(args.Path, args.Index)
	}

	return err
}
