package hdfs

import (
	"fmt"
	"os"
	"testing"
)

const (
	address  = "localhost:8020"
	username = ""
	path     = "/user/hive/warehouse/test"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient(username, address)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	files, err := client.client.ReadDir("/")
	switch err.(type) {
	case *os.PathError:
		println("path error")
		panic(err)
	case error:
		println("others error")
		panic(err)
	default:
		for _, file := range files {
			println(file.Name())
		}
	}
}

func TestClient_Ls(t *testing.T) {
	client, err := NewClient(username, address)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	dirs, err := client.LsDirs(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(dirs)
}
