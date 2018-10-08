package services

import (
	"strconv"

	core "github.com/timfpark/iceberg-core"
	goavro "gopkg.in/linkedin/goavro.v2"
)

type QueryService struct {
	StorageAdapter core.StorageAdapter
	Codec          *goavro.Codec
}

func (qs *QueryService) Query(partition string, startKey string, endKey string) (results []interface{}, err error) {
	startKeyInt, err := strconv.ParseInt(startKey, 10, 64)
	if err != nil {
		return nil, err
	}

	endKeyInt, err := strconv.ParseInt(endKey, 10, 64)
	if err != nil {
		return nil, err
	}

	return qs.StorageAdapter.Query(partition, startKeyInt, endKeyInt)
}

func (qs *QueryService) Init() (err error) {
	qs.StorageAdapter = &core.FilesystemStorageAdapter{
		BasePath:        "./test/fixtures/data",
		Codec:           qs.Codec,
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
	}

	return nil
}
