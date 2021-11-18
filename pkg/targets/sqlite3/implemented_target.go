package sqlite3

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &sqlite3Target{}
}

type sqlite3Target struct {
}

func (t *sqlite3Target) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
}

func (t *sqlite3Target) TargetName() string {
	return constants.FormatSqlite3
}

func (t *sqlite3Target) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *sqlite3Target) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
