package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
)

var (
	poolName      string
	writeFileName string
)

func init() {
	flag.StringVar(&poolName, "pool-name", "pool", "pool name")
	flag.StringVar(&writeFileName, "write-file-name", "focal-server-cloudimg-amd64.img", "filename for write test")
	flag.Parse()
}

// poolの一覧を取得する
func listPools(conn *rados.Conn) {
	pools, err := conn.ListPools()
	if err != nil {
		panic(fmt.Sprint("error ListPools: ", err))
	}
	fmt.Println("pools:", pools)
}

// ioctxのpoolにあるイメージの一覧を取得する
func listImages(ioctx *rados.IOContext) {
	imageNames, err := rbd.GetImageNames(ioctx)
	if err != nil {
		panic(fmt.Sprint("error GetImageNames", err))
	}
	fmt.Println("images:", imageNames)
}

// ioctxのpoolに空のイメージを作成する
func createImage(ioctx *rados.IOContext) {
	// 第2引数: イメージ名]
	// 第3引数: サイズ
	// 第4引数: オーダー(よくわからない)
	// 第5引数以降: args
	image, err := rbd.Create(ioctx, "tesimage1", 1024*1024*1024, 20)
	if err != nil {
		log.Fatal(err)
	}
	defer image.Close()
}

// データの書き込みテスト
func writeImage(ioctx *rados.IOContext) {
	// 書き込むファイル
	file, err := os.Open(writeFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// 書き込み先のイメージを取得
	image := rbd.GetImage(ioctx, "tesimage1")
	if err := image.Open(); err != nil {
		log.Fatal(err)
	}
	defer image.Close()

	// 書き込み
	if _, err := io.Copy(image, file); err != nil {
		log.Fatal(err)
	}
}

func main() {
	conn, err := rados.NewConn()
	if err != nil {
		panic(fmt.Sprint("error NewConn: ", err))
	}

	// ReadDefaultConfigFileは/etc/ceph.confを見る
	//err = conn.ReadDefaultConfigFile()
	err = conn.ReadConfigFile("./ceph.conf")
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

	ioctx, err := conn.OpenIOContext(poolName)
	if err != nil {
		panic(fmt.Sprint("error OpenIOContext:", err))
	}
	defer ioctx.Destroy()

	createImage(ioctx)
	writeImage(ioctx)
	listImages(ioctx)
}
