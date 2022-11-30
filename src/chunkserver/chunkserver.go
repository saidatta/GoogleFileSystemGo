package chunkserver

import (
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"encoding/gob"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex
	address  gfs.ServerAddress // currentStoredChunks-server address
	master   gfs.ServerAddress // master address
	rootDir  string            // path to data storage
	l        net.Listener
	shutdown chan struct{}

	dl                     *downloadBuffer                // expiring download buffer
	currentStoredChunks    map[gfs.ChunkHandle]*chunkInfo // currentStoredChunks information
	dead                   bool                           // set to ture if server is shutdown
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	garbage                []gfs.ChunkHandle              // garbages
}

// Mutation - A mutation is an operation that changes the contents or metadata of a chunk such as a write or an append operation
type Mutation struct {
	mtype  gfs.MutationType
	data   []byte
	offset gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length    gfs.Offset
	version   gfs.ChunkVersion // version number of the currentStoredChunks in disk
	checksum  gfs.Checksum
	mutations map[gfs.ChunkVersion]*Mutation // mutation buffer
	abandoned bool                           // unrecoverable error
}

const (
	MetaFileName = "gfs-server.meta"
	FilePerm     = 0755
)

// NewAndServe starts a currentStoredChunks-server and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, rootDir string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		rootDir:                rootDir,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		currentStoredChunks:    make(map[gfs.ChunkHandle]*chunkInfo),
	}

	rpcs := rpc.NewServer()
	_ = rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("currentStoredChunks-server listen error:", e)
	}
	cs.l = l

	// Mkdir
	_, err := os.Stat(rootDir)
	if err != nil { // not exist
		err := os.Mkdir(rootDir, FilePerm)
		if err != nil {
			log.Fatal("error in mkdir ", err)
		}
	}

	err = cs.loadMeta()
	if err != nil {
		log.Warning("Error in load metadata: ", err)
	}

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					// you forward the connection and serve it. Then immediately close the connection.
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !cs.dead {
					log.Fatal("currentStoredChunks-server encountered error: ", err)
				}
			}
		}
	}()

	// Background Activity
	// heartbeat, store persistent meta, garbage collection ...
	go func() {
		heartbeatTicker := time.Tick(gfs.HeartbeatInterval)
		storeTicker := time.Tick(gfs.ServerStoreInterval)
		garbageTicker := time.Tick(gfs.GarbageCollectionInt)
		quickStart := make(chan bool, 1) // send first heartbeat right away..
		quickStart <- true
		for {
			var err error
			var branch string
			select {
			case <-cs.shutdown:
				return
			case <-quickStart:
				branch = "heartbeat"
				err = cs.heartbeat()
			case <-heartbeatTicker:
				branch = "heartbeat"
				err = cs.heartbeat()
			case <-storeTicker:
				branch = "storemeta"
				err = cs.storeMeta()
			case <-garbageTicker:
				branch = "garbagecollecton"
				err = cs.garbageCollection()
			}

			if err != nil {
				log.Errorf("%v background(%v) error %v", cs.address, branch, err)
			}
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, leader addr = %v", addr, rootDir, masterAddr)

	return cs
}

func (cs *ChunkServer) storeMeta() error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	for handle, ck := range cs.currentStoredChunks {
		metas = append(metas, gfs.PersistentChunkInfo{
			Handle: handle, Length: ck.length, Version: ck.version,
		})
	}

	log.Infof("Server %v : store metadata len: %v", cs.address, len(metas))
	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)

	return err
}

// loadMeta loads metadata from disk
func (cs *ChunkServer) loadMeta() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	dec := gob.NewDecoder(file)
	err = dec.Decode(&metas)
	if err != nil {
		return err
	}

	log.Infof("Server %v : load metadata len: %v", cs.address, len(metas))

	// load currentStoredChunks info into memory
	for _, ck := range metas {
		log.Infof("Server %v restore %v version: %v length: %v", cs.address, ck.Handle, ck.Version, ck.Length)
		cs.currentStoredChunks[ck.Handle] = &chunkInfo{
			length:  ck.Length,
			version: ck.Version,
		}
	}

	return nil
}

// garbage collection  Note: no lock are needed, since the background activities are single thread
func (cs *ChunkServer) garbageCollection() error {
	for _, v := range cs.garbage {
		cs.deleteChunk(v)
	}

	cs.garbage = make([]gfs.ChunkHandle, 0)
	return nil
}

// deleteChunk deletes a currentStoredChunks during garbage collection
func (cs *ChunkServer) deleteChunk(handle gfs.ChunkHandle) error {
	cs.lock.Lock()
	delete(cs.currentStoredChunks, handle)
	cs.lock.Unlock()

	filename := path.Join(cs.rootDir, fmt.Sprintf("currentStoredChunks%v.chk", handle))
	err := os.Remove(filename)
	return err
}

// heartbeat calls master regularly to report currentStoredChunks-server's status
func (cs *ChunkServer) heartbeat() error {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]gfs.ChunkHandle, len(pe))
	for i, v := range pe {
		le[i] = v.(gfs.ChunkHandle)
	}
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: le,
	}
	var r gfs.HeartbeatResponse
	err := util.Execute(cs.master, "Leader.RPCHeartbeat", args, &r)
	if err != nil {
		return err
	}

	cs.garbage = append(cs.garbage, r.Garbage...)
	return err
}

// RPCReportSelf reports all chunks the server holds
func (cs *ChunkServer) RPCReportSelf(reply *gfs.ReportSelfResponse) error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	log.Debug(cs.address, " report collection started")
	var allExistingChunks []gfs.PersistentChunkInfo
	for handle, targetChunk := range cs.currentStoredChunks {
		allExistingChunks = append(allExistingChunks, gfs.PersistentChunkInfo{
			Handle:   handle,
			Version:  targetChunk.version,
			Length:   targetChunk.length,
			Checksum: targetChunk.checksum,
		})
	}
	reply.Chunks = allExistingChunks
	log.Debug(cs.address, " report collection ended")

	return nil
}

// RPCCheckVersion is called by leader to check chunk version and detect stale chunk
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionResponse) error {
	cs.lock.RLock()
	targetChunk, ok := cs.currentStoredChunks[args.Handle]
	cs.lock.RUnlock()
	if !ok || targetChunk.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", args.Handle)
	}

	targetChunk.Lock()
	defer targetChunk.Unlock()

	if targetChunk.version+gfs.ChunkVersion(1) == args.Version {
		targetChunk.version++
		reply.Stale = false
	} else {
		log.Warningf("%v : stale chunk %v", cs.address, args.Handle)
		targetChunk.abandoned = true
		reply.Stale = true
	}

	return nil
}

// RPCForwardData is called by client or another replica who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataResponse) error {
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("Data %v already exists", args.DataID)
	}

	cs.dl.Set(args.DataID, args.Data)

	// remove the node address from the chainOrder, and use it to forward the data to be replicated to it.
	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		err := util.Execute(next, "ChunkServer.RPCForwardData", args, reply)
		return err
	}

	return nil
}

// RPCCreateChunk is called by Leader to create a new chunk given the chunk handle with no data.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	log.Infof("Server %v : create chunk %v", cs.address, args.Handle)

	if _, ok := cs.currentStoredChunks[args.Handle]; ok {
		// if chunk exists in chunk-server skip.
		log.Warning("[ignored] recreate a chunk in RPCCreateChunk")
		return nil // TODO : error handle
		//return fmt.Errorf("Chunk %v already exists", args.Handle)
	}

	cs.currentStoredChunks[args.Handle] = &chunkInfo{
		length: 0,
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, response *gfs.ReadChunkResponse) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	// read from disk
	var err error
	response.Data = make([]byte, args.Length)
	ck.RLock()
	response.Length, err = cs.readChunk(handle, args.Offset, response.Data)
	ck.RUnlock()
	if err == io.EOF {
		response.ErrorCode = gfs.ReadEOF
		return nil
	}

	if err != nil {
		return err
	}
	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkResponse) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	newLen := args.Offset + gfs.Offset(len(data))
	if newLen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), gfs.MaxChunkSize)
	}

	targetHandle := args.DataID.Handle
	cs.lock.RLock()
	targetChunk, ok := cs.currentStoredChunks[targetHandle]
	cs.lock.RUnlock()
	if !ok || targetChunk.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", targetHandle)
	}

	if err = func() error {
		targetChunk.Lock()
		defer targetChunk.Unlock()
		mutation := &Mutation{gfs.MutationWrite, data, args.Offset}

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(targetHandle, mutation)
		}()

		// call secondaries
		callArgs := gfs.ApplyMutationArg{Mtype: gfs.MutationWrite, DataID: args.DataID, Offset: args.Offset}
		err = util.ExecuteAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(targetHandle)

	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within 1/4 chunk size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, response *gfs.AppendChunkResponse) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("Append data size %v excceeds max append size %v", len(data), gfs.MaxAppendSize)
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	var mtype gfs.MutationType

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		newLen := ck.length + gfs.Offset(len(data))
		offset := ck.length
		if newLen > gfs.MaxChunkSize {
			mtype = gfs.MutationPad
			ck.length = gfs.MaxChunkSize
			response.ErrorCode = gfs.AppendExceedChunkSize
		} else {
			mtype = gfs.MutationAppend
			ck.length = newLen
		}
		response.Offset = offset

		mutation := &Mutation{mtype, data, offset}

		// apply to local
		// after 1 channel error propagated, they're
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := gfs.ApplyMutationArg{Mtype: mtype, DataID: args.DataID, Offset: offset}
		err = util.ExecuteAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(handle)

	return nil
}

// RPCSendCopy is called by leader, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.RLock()
	defer ck.RUnlock()

	log.Infof("Server %v : Send copy of %v to %v", cs.address, handle, args.Address)
	data := make([]byte, ck.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}

	var r gfs.ApplyCopyResponse
	err = util.Execute(args.Address, "ChunkServer.RPCApplyCopy", gfs.ApplyCopyArg{handle, data, ck.version}, &r)
	if err != nil {
		return err
	}

	return nil
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	log.Infof("Server %v : Apply copy of %v", cs.address, handle)

	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0)
	if err != nil {
		return err
	}
	log.Infof("Server %v : Apply done", cs.address)
	return nil
}

// RPCApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	mutation := &Mutation{args.Mtype, data, args.Offset}
	err = func() error {
		ck.Lock()
		defer ck.Unlock()
		err = cs.doMutation(handle, mutation)
		return err
	}()

	return err
}

// apply mutations (write, append, pad) in currentStoredChunks buffer in proper order according to version number
func (cs *ChunkServer) doMutation(handle gfs.ChunkHandle, m *Mutation) error {
	var err error
	if m.mtype == gfs.MutationPad {
		data := []byte{0}
		// pad the data with random.
		err = cs.writeChunk(handle, data, gfs.MaxChunkSize-1)
	} else {
		err = cs.writeChunk(handle, m.data, m.offset)
	}

	if err != nil {
		cs.lock.RLock()
		ck := cs.currentStoredChunks[handle]
		cs.lock.RUnlock()
		log.Warningf("%v abandon currentStoredChunks %v", cs.address, handle)
		ck.abandoned = true
		return err
	}

	return nil
}

// writeChunk writes data at offset to a currentStoredChunks at disk
func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset) error {
	cs.lock.RLock()
	ck := cs.currentStoredChunks[handle]
	cs.lock.RUnlock()

	// ck is already locked in top caller
	newLen := offset + gfs.Offset(len(data))
	if newLen > ck.length {
		ck.length = newLen
	}

	if newLen > gfs.MaxChunkSize {
		log.Fatal("new length > gfs.MaxChunkSize")
	}

	log.Infof("Server %v : write to currentStoredChunks %v at %v len %v", cs.address, handle, offset, len(data))
	filename := path.Join(cs.rootDir, fmt.Sprintf("currentStoredChunks%v.chk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

// readChunk reads data at offset from a chunk at disk
func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))

	f, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	log.Infof("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	return f.ReadAt(data, int64(offset))
}

// Shutdown terminates the currentStoredChunks-server and shut's it down
//func (cs *ChunkServer) Shutdown(args gfs.Nouse, reply *gfs.Nouse) error {
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warning(cs.address, "is being shutdown")
		cs.dead = true
		// closing down the shutdown channel.
		close(cs.shutdown)
		// closing the net listener down.
		cs.l.Close()
	}
	err := cs.storeMeta()
	if err != nil {
		log.Warning("error in storing metadata: ", err)
	}
}
