package main

import (
	"bufio"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

type benchmark struct {
	ds     *fileDataSource
	dbName string
}

func NewBenchmark(dbName string, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds *fileDataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
	} else {
		panic("Must be a file!")
	}

	return &benchmark{
		ds:     ds,
		dbName: dbName,
	}, nil
}

func newFileDataSource(fileName string) *fileDataSource {
	br := load.GetBufferedReader(fileName)
	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetDataSource() targets.DataSource {
	// return b.ds
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(config.FileName))}

}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		dbName: "SQLite3DB",
		ds:     b.ds,
	}
}
