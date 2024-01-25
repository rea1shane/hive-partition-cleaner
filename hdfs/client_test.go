package hdfs

import (
	"fmt"
	"os"
	"testing"
)

const (
	address  = "localhost:8020"
	username = ""
	path     = "hdfs://namenode:8020/user/hive/warehouse/test"
)

var (
	targetPartitions = []string{"`date`=20231111", "`date`=20231112"}
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

func TestClient_ListPartitions(t *testing.T) {
	client, err := NewClient(username, address)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	partitions, err := client.ListPartitions(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(partitions)
}

func TestClient_DeletePartitions(t *testing.T) {
	client, err := NewClient(username, address)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	err = client.DeletePartitions(path, targetPartitions)
	if err != nil {
		panic(err)
	}
}

func TestFormatPath(t *testing.T) {
	res, err := formatPath(path)
	if err != nil {
		panic(err)
	}
	println(res)
}
