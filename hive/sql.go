package hive

import (
	"fmt"
	"strings"
)

// GenerateDescFormattedSql 生成
func GenerateDescFormattedSql(db, table string) string {
	return fmt.Sprintf("DESC FORMATTED %s.%s", db, table)
}

// GenerateShowPartitionsSql 生成列出所有分区的 SQL
func GenerateShowPartitionsSql(db, table string) string {
	return fmt.Sprintf("SHOW PARTITIONS %s.%s", db, table)
}

// GenerateAlterPartitionsSql 生成修改目标表的目标分区的 SQL
func GenerateAlterPartitionsSql(db, table string, partitions []string) string {
	if len(partitions) == 0 {
		return ""
	}

	// 构建 SQL
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s DROP IF EXISTS ", db, table))
	for _, partition := range partitions {
		sqlBuilder.WriteString(fmt.Sprintf("PARTITION (%s), ", partition))
	}
	sql := sqlBuilder.String()

	return sql[:len(sql)-2]
}
