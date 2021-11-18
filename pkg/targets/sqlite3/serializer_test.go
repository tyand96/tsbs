package sqlite3

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestSqliteSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			// We have a CREATE TABLE line here because it's the first time the test has seen the 'cpu' measurement
			Output: "CREATE TABLE cpu (timestamp INTEGER NOT NULL,hostname TEXT,region TEXT,datacenter TEXT,usage_guest_nice REAL);^\nINSERT INTO cpu (timestamp,hostname,region,datacenter,usage_guest_nice) VALUES (1451606400000000000,\"host_0\",\"eu_west_1\",\"eu_west_1b\",38.24311829);^1\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "INSERT INTO cpu (timestamp,hostname,region,datacenter,usage_guest) VALUES (1451606400000000000,\"host_0\",\"eu_west_1\",\"eu_west_1b\",38);^1\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output:     "INSERT INTO cpu (timestamp,hostname,region,datacenter,big_usage_guest,usage_guest,usage_guest_nice) VALUES (1451606400000000000,\"host_0\",\"eu_west_1\",\"eu_west_1b\",5000000000,38,38.24311829);^3\n",
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "INSERT INTO cpu (timestamp,usage_guest_nice) VALUES (1451606400000000000,38.24311829);^1\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
