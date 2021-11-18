package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
)

// Struct for the statements
type Statement struct {
	s string
}

// Global vars
var (
	loader  load.BenchmarkRunner
	config  load.BenchmarkRunnerConfig
	bufPool sync.Pool
	target  targets.ImplementedTarget
)

// Allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatSqlite3)
	config = load.BenchmarkRunnerConfig{}
	// Not all the default flags apply to SQLite3
	pflag.CommandLine.Uint("batch-size", 10000, "Number of items to batch together in a sigle insert")
	pflag.CommandLine.Uint("workers", 1, "Number of parallel clients inserting")
	pflag.CommandLine.Uint64("limit", 0, "Number of items to insert (0 = all of them).")
	pflag.CommandLine.Bool("do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	pflag.CommandLine.Duration("reporting-period", 10*time.Second, "Period to report write stats")
	pflag.CommandLine.String("file", "", "File name to read data from")
	pflag.CommandLine.Int64("seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	pflag.CommandLine.String("insert-intervals", "", "Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s between inserts, worker 2 and others wait 2s")
	pflag.CommandLine.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")
	// target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return new(Statement)
		},
	}

	// bufPool = sync.Pool{
	// 	New: func() interface{} {
	// 		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	// 	},
	// }

	benchmark, err := NewBenchmark("SQLite3DB", &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: config.FileName},
	})

	if err != nil {
		panic(err)
	}

	loader.RunBenchmark(benchmark)
}
