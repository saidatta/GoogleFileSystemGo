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

func newLeaseBuffer(leaderAddr gfs.ServerAddress, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer: make(map[gfs.ChunkHandle]*gfs.Lease),
		tick:   tick,
		leader: leaderAddr,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.Expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf

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

		lease = &gfs.Lease{Primary: l.Primary, Expire: l.Expire, Secondaries: l.Secondaries}
		buf.buffer[handle] = lease
		return lease, nil
	}

	return lease, nil
}
