package util

import (
	"GFSGoLang/src/gfs"
	"fmt"
	"math/rand"
	"net/rpc"
)

// Execute is RPC call helper
func Execute(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, encounteredError := rpc.Dial("tcp", string(srv))
	if encounteredError != nil {
		return encounteredError
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}

// ExecuteAll applies the rpc call to all destinations.
func ExecuteAll(allDestinations []gfs.ServerAddress, rpcname string, args interface{}) error {
	ch := make(chan error)
	for _, d := range allDestinations {
		go func(addr gfs.ServerAddress) {
			ch <- Execute(addr, rpcname, args, nil)
		}(d)
	}
	errList := ""
	for _ = range allDestinations {
		if err := <-ch; err != nil {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// FindRandomKServers randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func FindRandomKServers(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}
