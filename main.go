package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
)

func listPools(conn *rados.Conn) {
	pools, err := conn.ListPools()
	if err != nil {
		panic(fmt.Sprint("error ListPools: ", err))
	}
	fmt.Println("pools:", pools)
}

func listImages(ioctx *rados.IOContext) {
	imageNames, err := rbd.GetImageNames(ioctx)
	if err != nil {
		panic(fmt.Sprint("error GetImageNames", err))
	}
	fmt.Println("images:", imageNames)
}

func main() {
	conn, err := rados.NewConn()
	if err != nil { 
		panic(fmt.Sprint("error NewConn: ", err))
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		panic(fmt.Sprint("error ReadDefaultConfigFile: ", err))
	}

	err = conn.Connect()
	if err != nil {
		panic(fmt.Sprint("error Connect: ", err))
	}
	defer conn.Shutdown()

	fmt.Println("success connect ceph cluster")
	
	listPools(conn)

	ioctx, err := conn.OpenIOContext("rbd")
	if err != nil {
		panic(fmt.Sprint("error OpenIOContext:", err))
	}
	ioctx.Destroy()

	listImages(ioctx)
}

