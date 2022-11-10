package client

import (
	"GFSGoLang/src/gfs"
	"GFSGoLang/src/util"
	"sync"
	"time"
)

type leaseBuffer struct {
	sync.RWMutex
	leader gfs.ServerAddress
	buffer map[gfs.ChunkHandle]*gfs.Lease

	tick time.Duration
}

func (buf *leaseBuffer) Get(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]

	if !ok { // ask leader to send one
		var l gfs.GetPrimaryAndSecondariesResponse
		err := util.Execute(buf.leader, "Leader.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{Handle: handle}, &l)
		if err != nil {
			return nil, err
		}

		lease = &gfs.Lease{l.Primary, l.Expire, l.Secondaries}
		buf.buffer[handle] = lease
		return lease, nil
	}
	// extend lease (it is the work of chunk server)
	/*
	   go func() {
	       var r gfs.ExtendLeaseReply
	       util.Call(buf.leader, "Leader.RPCExtendLease", gfs.ExtendLeaseArg{handle, lease.Primary}, &r)
	       lease.Expire = r.Expire
	   }()
	*/
	return lease, nil
}
