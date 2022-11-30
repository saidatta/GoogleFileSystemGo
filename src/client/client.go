package client

import (
	"GFSGoLang/src/chunkserver"
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
)

type Client struct {
	leader      gfs.ServerAddress
	leaseBuffer *leaseBuffer
}

func NewClient(leaderAddr gfs.ServerAddress) *Client {
	return &Client{
		leader:      leaderAddr,
		leaseBuffer: newLeaseBuffer(leaderAddr, gfs.LeaseBufferTick),
	}
}

//Create creates file in the specified path.
func (c *Client) Create(path gfs.Path) error {
	var response gfs.CreateFileResponse

	err := util.Execute(c.leader, "Leader.RPCCreateFile", gfs.CreateFileArg{Path: path}, &response)
	if err != nil {
		return err
	}
	return nil
}

//Delete deletes file path
func (c *Client) Delete(path gfs.Path) error {
	var response gfs.DeleteFileResponse
	err := util.Execute(c.leader, "Leader.RPCDeleteFile", gfs.DeleteFileArg{Path: path}, &response)
	if err != nil {
		return err
	}
	return nil

}

//Rename renames fileDirectories within the specified paths.
func (c *Client) Rename(source gfs.Path, target gfs.Path) error {
	var response gfs.RenameFileResponse
	err := util.Execute(c.leader, "Leader.RPCRenameFile", gfs.RenameFileArg{SourceName: source, TargetName: target}, &response)

	if err != nil {
		return err
	}

	return nil
}

// Mkdir makes a new directory
func (c *Client) Mkdir(path gfs.Path) error {
	var response gfs.MkdirResponse
	err := util.Execute(c.leader, "Leader.RPCMkdir", gfs.MkdirArg{Path: path}, &response)
	if err != nil {
		return err
	}
	return nil
}

// List lists all files in specific directory
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	var response gfs.ListResponse
	err := util.Execute(c.leader, "Leader.RPCList", gfs.ListArg{Path: path}, &response)
	if err != nil {
		return nil, err
	}
	return response.Files, nil
}

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it return the number of bytes and an error.
// the error is set to io.EOF if stream meets the end of file
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	var f gfs.GetFileInfoResponse
	err = util.Execute(c.leader, "Leader.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return -1, err
	}

	//illegal offset.
	if int64(offset/gfs.MaxChunkSize) > f.TotalChunks {
		return -1, fmt.Errorf("read offset exceeds file size")
	}

	currentPosition := 0
	for currentPosition < len(data) {
		startingIndexInChunk := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffsetWithinChunk := offset % gfs.MaxChunkSize

		// if index > the number of chunks available.
		if int64(startingIndexInChunk) >= f.TotalChunks {
			err = gfs.Error{Code: gfs.ReadEOF, Err: "EOF over chunks"}
			break
		}

		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, startingIndexInChunk)
		if err != nil {
			return
		}

		var n int
		for {
			n, err = c.ReadChunk(handle, chunkOffsetWithinChunk, data[currentPosition:])
			if err == nil || err.(gfs.Error).Code == gfs.ReadEOF {
				break
			}
			log.Warning("Read ", handle, " connection error, try again: ", err)
		}

		offset += gfs.Offset(n)
		currentPosition += n
		if err != nil {
			break
		}
	}

	if err != nil && err.(gfs.Error).Code == gfs.ReadEOF {
		return currentPosition, io.EOF
	} else {
		return currentPosition, err
	}
}

// Write is a client API. write data to file at specific offset
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	var f gfs.GetFileInfoResponse
	err := util.Execute(c.leader, "Leader.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return err
	}

	if int64(offset/gfs.MaxChunkSize) > f.TotalChunks {
		return fmt.Errorf("write offset exceeds file size")
	}

	begin := 0
	for {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize

		handle, err := c.GetChunkHandle(path, index)
		if err != nil {
			return err
		}

		writeMax := int(gfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		for {
			err = c.WriteChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
			log.Warning("Write ", handle, "  connection error, try again ", err)
		}
		if err != nil {
			return err
		}

		offset += gfs.Offset(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}

	return nil
}

// helper FUNCTIONS

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, leader will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	var response gfs.GetChunkHandleResponse
	err := util.Execute(c.leader, "Leader.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &response)
	if err != nil {
		return 0, err
	}
	return response.Handle, nil
}

// ReadChunk read data from the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	var readLen int

	if gfs.MaxChunkSize-offset > gfs.Offset(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(gfs.MaxChunkSize - offset)
	}

	var l gfs.GetReplicasResponse
	err := util.Execute(c.leader, "Leader.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &l)
	if err != nil {
		return 0, gfs.Error{gfs.UnknownError, err.Error()}
	}
	// any random replica is fine.
	locReplicaAddress := l.Locations[rand.Intn(len(l.Locations))]
	if len(l.Locations) == 0 {
		return 0, gfs.Error{gfs.UnknownError, "no replica"}
	}

	var r gfs.ReadChunkResponse
	r.Data = data
	err = util.Execute(locReplicaAddress, "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen}, &r)
	if err != nil {
		return 0, gfs.Error{gfs.UnknownError, err.Error()}
	}
	if r.ErrorCode == gfs.ReadEOF {
		return r.Length, gfs.Error{gfs.ReadEOF, "read EOF"}
	}
	return r.Length, nil
}

// WriteChunk writes data to the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), gfs.MaxChunkSize)
	}

	l, err := c.leaseBuffer.Get(handle)
	if err != nil {
		return err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Secondaries, l.Primary)

	var d gfs.ForwardDataResponse
	err = util.Execute(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]}, &d)
	if err != nil {
		return err
	}

	wcargs := gfs.WriteChunkArg{DataID: dataID, Offset: offset, Secondaries: l.Secondaries}
	err = util.Execute(l.Primary, "ChunkServer.RPCWriteChunk", wcargs, &gfs.WriteChunkResponse{})
	return err
}

// AppendChunk appends data to a chunk.
// if success, Chunk offset of the start point of data will be returned.
// <code>len(data)</code> should be within 1/4 chunk size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: fmt.Sprintf("len(data) = %v > max append size %v", len(data), gfs.MaxAppendSize)}
	}

	log.Infof("Client : get lease ")

	l, err := c.leaseBuffer.Get(handle)
	if err != nil {
		return -1, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Secondaries, l.Primary)

	log.Warning("Client : get locations %v", chain)
	var d gfs.ForwardDataResponse
	err = util.Execute(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]}, &d)
	if err != nil {
		return -1, gfs.Error{gfs.UnknownError, err.Error()}
	}

	log.Warning("Client : send append request to primary. data : %v", dataID)

	var a gfs.AppendChunkResponse
	appendChunkArgs := gfs.AppendChunkArg{DataID: dataID, Secondaries: l.Secondaries}
	err = util.Execute(l.Primary, "ChunkServer.RPCAppendChunk", appendChunkArgs, &a)
	if err != nil {
		return -1, gfs.Error{gfs.UnknownError, err.Error()}
	}
	if a.ErrorCode == gfs.AppendExceedChunkSize {
		return a.Offset, gfs.Error{a.ErrorCode, "append over chunks"}
	}
	return a.Offset, nil
}
