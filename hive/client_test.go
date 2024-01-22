package hive

import (
	"context"
	"fmt"
	"testing"
)

const (
	host            = "localhost"
	port            = 10000
	zookeeperQuorum = ""
	username        = ""
	password        = ""
)

func TestNewClient(t *testing.T) {
	client, err := NewClient(host, port, username, password)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	cursor := client.connection.Cursor()
	defer cursor.Close()
	ctx := context.Background()
	cursor.Exec(ctx, "show databases")
	if cursor.Error() != nil {
		panic(cursor.Error())
	}
	var db string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &db)
		if cursor.Error() != nil {
			panic(cursor.Error())
		}
		fmt.Println(db)
	}
}

func TestClient_GetLocation(t *testing.T) {
	client, err := NewClient(host, port, username, password)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	location, err := client.GetLocation(context.Background(), db, table)
	if err != nil {
		panic(err)
	}
	println(location)
}

func TestClient_ShowPartitions(t *testing.T) {
	client, err := NewClient(host, port, username, password)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	partitions, err := client.ShowPartitions(context.Background(), db, table)
	if err != nil {
		panic(err)
	}
	fmt.Println(partitions)
}

// TODO fix
func TestClient_AlterPartitions(t *testing.T) {
	client, err := NewClient(host, port, username, password)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	err = client.AlterPartitions(context.Background(), db, table, targetPartitions)
	if err != nil {
		panic(err)
	}
}
