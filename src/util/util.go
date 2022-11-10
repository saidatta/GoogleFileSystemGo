package util

import (
	"GFSGoLang/src/gfs"
	"net/rpc"
)

// Execute is RPC call helper
func Execute(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}
