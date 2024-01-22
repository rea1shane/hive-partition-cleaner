package hive

import (
	"context"
	"errors"
	"github.com/beltran/gohive"
)

type Client struct {
	connection *gohive.Connection
}

// NewClient 新建一个 Hive 客户端
func NewClient(host string, port int, username, password string) (*Client, error) {
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	configuration.Password = password
	connection, err := gohive.Connect(host, port, "NONE", configuration)
	if err != nil {
		return nil, err
	}
	return &Client{connection: connection}, nil
}

// NewClientByZookeeper 通过 ZooKeeper 新建一个 Hive 客户端
func NewClientByZookeeper(zookeeperQuorum, username, password string) (*Client, error) {
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	configuration.Password = password
	connection, err := gohive.ConnectZookeeper(zookeeperQuorum, "NONE", configuration)
	if err != nil {
		return nil, err
	}
	return &Client{connection: connection}, nil
}

// Close 关闭客户端
func (c *Client) Close() error {
	return c.connection.Close()
}

func (c *Client) GetLocation(ctx context.Context, db, table string) (string, error) {
	// 新建一个 cursor
	cursor := c.connection.Cursor()
	defer cursor.Close()

	// 查询模式
	cursor.Exec(ctx, GenerateDescFormattedSql(db, table))
	if cursor.Error() != nil {
		return "", cursor.Error()
	}

	// 解析结果
	var colName, dataType, comment string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &colName, &dataType, &comment)
		if cursor.Error() != nil {
			return "", cursor.Error()
		}
		if colName == "Location:           " {
			return dataType, nil
		}
	}

	return "", errors.New("not found")
}

// ShowPartitions 获取指定表的所有分区
func (c *Client) ShowPartitions(ctx context.Context, db, table string) ([]string, error) {
	// 新建一个 cursor
	cursor := c.connection.Cursor()
	defer cursor.Close()

	// 查询所有的分区
	cursor.Exec(ctx, GenerateShowPartitionsSql(db, table))
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

// AlterPartitions 修改目标表的目标分区
func (c *Client) AlterPartitions(ctx context.Context, db, table string, partitions []string) error {
	sql := GenerateAlterPartitionsSql(db, table, partitions)
	if "" == sql {
		return nil
	}

	// 新建一个 cursor
	cursor := c.connection.Cursor()
	defer cursor.Close()

	// 执行 SQL
	cursor.Exec(ctx, sql)
	return cursor.Error()
}
