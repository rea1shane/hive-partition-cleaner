package hive

import "testing"

var (
	db               = "default"
	table            = "test"
	targetPartitions = []string{"date=20231111", "date=20231112"}
)

func TestGenerateDescFormattedSql(t *testing.T) {
	println(GenerateDescFormattedSql(db, table))
}

func TestGenerateShowPartitionsSql(t *testing.T) {
	println(GenerateShowPartitionsSql(db, table))
}

func TestGenerateAlterPartitionsSql(t *testing.T) {
	println(GenerateAlterPartitionsSql(db, table, targetPartitions))
}
