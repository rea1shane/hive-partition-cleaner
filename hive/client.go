package hive

import (
	"context"
	"fmt"
	"github.com/beltran/gohive"
)

type Client struct {
	connection *gohive.Connection
}

// NewClient 新建一个 Hive 客户端
func NewClient(host string, port int, username, password string) (Client, error) {
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	configuration.Password = password
	connection, err := gohive.Connect(host, port, "NONE", configuration)
	if err != nil {
		return Client{}, err
	}
	return Client{connection: connection}, nil
}

// NewClientByZookeeper 通过 ZooKeeper 新建一个 Hive 客户端
func NewClientByZookeeper(zookeeperQuorum, username, password string) (Client, error) {
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	configuration.Password = password
	connection, err := gohive.ConnectZookeeper(zookeeperQuorum, "NONE", configuration)
	if err != nil {
		return Client{}, err
	}
	return Client{connection: connection}, nil
}

// Close 关闭客户端
func (c *Client) Close() error {
	return c.connection.Close()
}

// ListPartitions 获取指定表的所有分区
func (c Client) ListPartitions(ctx context.Context, db, table string) ([]string, error) {
	// 新建一个 cursor
	cursor := c.connection.Cursor()
	defer cursor.Close()

	// 查询所有的分区
	cursor.Exec(ctx, fmt.Sprintf("SHOW PARTITIONS %s.%s", db, table))
	if cursor.Error() != nil {
		return nil, cursor.Error()
	}

	// 解析结果
	var (
		partitions []string
		partition  string
	)
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &partition)
		if cursor.Error() != nil {
			return nil, cursor.Error()
		}
		partitions = append(partitions, partition)
	}

	return partitions, nil
}
