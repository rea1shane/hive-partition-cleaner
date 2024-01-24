package hdfs

import (
	"github.com/colinmarc/hdfs"
)

type Client struct {
	client *hdfs.Client
}

// NewClient 新建一个 HDFS 客户端
func NewClient(username string, addresses ...string) (*Client, error) {
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: addresses,
		User:      username,
	})
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

// Close 关闭客户端
func (c *Client) Close() error {
	return c.client.Close()
}

// ListPartitions 列出指定表存储路径下的所有分区
func (c *Client) ListPartitions(path string) (partitions []string, err error) {
	files, err := c.client.ReadDir(path)
	if err != nil {
		return
	}
	for _, file := range files {
		if file.IsDir() {
			partitions = append(partitions, file.Name())
		}
	}
	return
}
