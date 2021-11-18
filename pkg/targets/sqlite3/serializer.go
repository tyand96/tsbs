package sqlite3

import (
	"fmt"
	"io"
	"strings"

	"github.com/timescale/tsbs/pkg/data/serialize"

	"github.com/timescale/tsbs/pkg/data"
)

// Really an empty struct to be used in the measurements map
var exists = struct{}{}

// Map to hold the measurements that we have read already.
// This is to only create one CREATE statement per measurement
var measurements = make(map[string]struct{})

// Serializer writes a Point in a serialized form for SQLite3.
// This is in the form:
//
// CREATE TABLE <measurementName> (timestamp INTEGER NOT NULL, <tag1> <tagType>,...,<tagN> <tagNType>,<field1> <field1Type>,...,<fieldM> <fieldMType>);^\n
// INSERT INTO <measurementName> (timestamp,<tag1>,...,<tagN>,<field1>,...,<fieldM>) VALUES (<timestamp>,<tag1Val>,...,<tagNVal>,<field1Val>,...,<fieldMVal>);^<numFields>\n
type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	// Check if we have seen this measurement before.
	// If not, then we will add a new table creation command to our file
	if _, ok := measurements[string(p.MeasurementName())]; !ok {
		measurements[string(p.MeasurementName())] = exists
		// CREATE TABLE <measurementName> (<schema>)
		buf := make([]byte, 0, 1024)
		buf = append(buf, "CREATE TABLE "...)
		buf = append(buf, p.MeasurementName()...)
		buf = append(buf, ' ')
		buf = append(buf, '(')
		buf = append(buf, "timestamp INTEGER NOT NULL"...)

		tagKeys := p.TagKeys()
		tagValues := p.TagValues()
		for i, v := range tagKeys {
			buf = append(buf, ',')
			buf = serialize.FastFormatAppend(v, buf)
			buf = append(buf, ' ')

			// Now to get the type of the tag value
			switch tagValues[i].(type) {
			case string:
				buf = append(buf, "TEXT"...)
			case int64:
				buf = append(buf, "INTEGER"...)
			case float64:
				buf = append(buf, "REAL"...)
			default:
				errMsg := fmt.Sprintf("Type not supported yet: %T", tagValues[i])
				panic(errMsg)
			}
		}

		fieldKeys := p.FieldKeys()
		fieldValues := p.FieldValues()
		for i, v := range fieldKeys {
			buf = append(buf, ',')
			buf = serialize.FastFormatAppend(v, buf)
			buf = append(buf, ' ')

			// Now to get the type of the tag value
			switch fieldValues[i].(type) {
			case string:
				buf = append(buf, "TEXT"...)
			case int64:
				buf = append(buf, "INTEGER"...)
			case float64:
				buf = append(buf, "REAL"...)
			default:
				errMsg := fmt.Sprintf("Type not supported yet: %T", fieldValues[i])
				panic(errMsg)
			}
		}

		buf = append(buf, ')')
		buf = append(buf, ';')

		// Append a '^' symbol to be consistent with other lines
		buf = append(buf, '^')

		// Inserting a newline character
		buf = append(buf, '\n')

		// Finally, let's write this as well
		_, err := w.Write(buf)
		if err != nil {
			return err
		}
	}

	// Now to create the lines that insert data into the tables
	buf := make([]byte, 0, 1024)
	buf = append(buf, "INSERT INTO "...)

	// Now to insert the tableName, which is the measurement name
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, ' ')

	// Insert column names, which are the keys
	buf = append(buf, '(')
	buf = append(buf, "timestamp"...)

	tagKeys := p.TagKeys()
	for _, v := range tagKeys {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}

	fieldKeys := p.FieldKeys()
	for _, v := range fieldKeys {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}

	buf = append(buf, ')')

	// Now to insert the values
	buf = append(buf, " VALUES "...)

	buf = append(buf, '(')

	// Insert the timestamp
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)

	tagValues := p.TagValues()
	for _, v := range tagValues {
		buf = append(buf, ',')
		// Check the type, because if it is a string then
		// we need to wrap it in quotes
		if str, ok := v.(string); ok {
			// Also, we need to remove the "-" and "." characters because they're
			// not allowed in SQL
			v = strings.Replace(str, "-", "_", -1)
			v = strings.Replace(v.(string), ".", "_", -1)
			v = "\"" + v.(string) + "\""
		}
		buf = serialize.FastFormatAppend(v, buf)
	}

	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		// Check the type, because if it is a string then
		// we need to wrap it in quotes
		if str, ok := v.(string); ok {
			// Also, we need to remove the "-" and "." characters because they're
			// not allowed in SQL
			v = strings.Replace(str, "-", "_", -1)
			v = strings.Replace(v.(string), ".", "_", -1)
			v = "\"" + v.(string) + "\""
		}
		buf = serialize.FastFormatAppend(v, buf)
	}

	buf = append(buf, ')')
	buf = append(buf, ';')

	// Lastly, we will write the number of metrics. This will be removed
	// in the Append function, when we will count this.
	// I BELIEVE THIS IS THE NUMBER OF FIELDS ONLY, BUT IT'S UNCLEAR TO ME
	buf = append(buf, '^')

	numMetrics := len(fieldValues)

	buf = serialize.FastFormatAppend(numMetrics, buf)

	buf = append(buf, '\n')

	_, err := w.Write(buf)

	return err

}
