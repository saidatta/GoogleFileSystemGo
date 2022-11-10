package gfs

import "time"

// handshake
type CheckVersionArg struct {
	Handle  ChunkHandle
	Version ChunkVersion
}
type CheckVersionResponse struct {
	Stale bool
}

// chunk IO
type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []ServerAddress
}
type ForwardDataResponse struct {
	ErrorCode ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkResponse struct {
	ErrorCode ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkResponse struct {
	ErrorCode ErrorCode
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
}
type AppendChunkResponse struct {
	Offset    Offset
	ErrorCode ErrorCode
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset Offset
}
type ApplyMutationResponse struct {
	ErrorCode ErrorCode
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkResponse struct {
	ErrorCode ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkResponse struct {
	Data      []byte
	Length    int
	ErrorCode ErrorCode
}

// re-replication
type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyResponse struct {
	ErrorCode ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyResponse struct {
	ErrorCode ErrorCode
}

// no use argument
type Nouse struct{}

/*
 *  leader
 */

// handshake
type HeartbeatArg struct {
	Address          ServerAddress // chunkserver address
	LeaseExtensions  []ChunkHandle // leases to be extended
	AbandondedChunks []ChunkHandle // unrecoverable chunks
}
type HeartbeatResponse struct {
	Garbage []ChunkHandle
}

type ReportSelfArg struct {
}
type ReportSelfResponse struct {
	Chunks []PersistentChunkInfo
}

// chunk info
type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle
}
type GetPrimaryAndSecondariesResponse struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type ExtendLeaseResponse struct {
	Expire time.Time
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasResponse struct {
	Locations []ServerAddress
}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoResponse struct {
	IsDir       bool
	Length      int64
	TotalChunks int64
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleResponse struct {
	Handle ChunkHandle
}

// namespace operation
type CreateFileArg struct {
	Path Path
}
type CreateFileResponse struct{}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileResponse struct{}

type RenameFileArg struct {
	SourceName Path
	TargetName Path
}
type RenameFileResponse struct{}

type MkdirArg struct {
	Path Path
}
type MkdirResponse struct{}

type ListArg struct {
	Path Path
}
type ListResponse struct {
	Files []PathInfo
}
