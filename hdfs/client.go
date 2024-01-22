package hdfs

import (
	"github.com/colinmarc/hdfs"
)

type Client struct {
	client *hdfs.Client
}

// NewClient 新建一个 HDFS 客户端
func NewClient(address, username string) (*Client, error) {
	client, err := hdfs.New(address)
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

// NewClientByHadoopConfig 通过 Hadoop 配置文件新建一个 HDFS 客户端
func NewClientByHadoopConfig(hadoopConfigPath, username string) (*Client, error) {
	hadoopConfig := hdfs.LoadHadoopConf(hadoopConfigPath)
	namenodes, err := hadoopConfig.Namenodes()
	if err != nil {
		return nil, err
	}
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: namenodes,
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
