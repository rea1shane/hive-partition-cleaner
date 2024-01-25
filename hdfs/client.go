package hdfs

import (
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"regexp"
	"strings"
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

// ListPartitions 列出指定表的存储路径下的所有分区
func (c *Client) ListPartitions(path string) (partitions []string, err error) {
	formattedPath, err := formatPath(path)
	if err != nil {
		return
	}

	files, err := c.client.ReadDir(formattedPath)
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

// DeletePartitions 删除指定表的存储路径下的指定分区
func (c *Client) DeletePartitions(path string, partitions []string) error {
	formattedPath, err := formatPath(path)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		err = c.client.Remove(fmt.Sprintf("%s/%s", formattedPath, partition))
		if err != nil {
			return err
		}
	}
	return nil
}

// formatPath 格式化路径
// 如果传入的路径为 / 开头，则直接返回
// 如果传入的路径为 hdfs://{cluster}/{path}，则返回 /{path} 部份
// 如果传入的路径不符合上述所有的规则，则返回非法路径错误
func formatPath(path string) (string, error) {
	if !strings.HasPrefix(path, "hdfs://") && strings.HasPrefix(path, "/") {
		return path, nil
	}

	pattern := `hdfs://[^/]*(/.*)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(path)
	if len(matches) >= 2 {
		return matches[1], nil
	} else {
		return "", errors.New("illegal path")
	}
}
