package main

import (
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

// This creates a Parquet file with an empty group to test the schema parsing fix.
//
// The schema should be:
//   message root {
//     optional int64 id;
//     optional group nested {          // Inner node with 1 child
//       optional group inner {         // Inner node with 0 children (EMPTY GROUP)
//       }
//     }
//     optional int32 optional_data;
//   }
//
// The key point: 'inner' is a group with num_children=0 and no type set.
// Old buggy code: if (num_children > 0) -> inner node (FAILS for empty group)
// New fixed code: if (!__isset.type) -> inner node (CORRECTLY handles empty group)

type Row struct {
	ID           int64 `parquet:"id,optional"`
	Nested       Nested `parquet:"nested,optional"`
	OptionalData int32 `parquet:"optional_data,optional"`
}

// Nested struct with an empty inner group
type Nested struct {
	Inner EmptyGroup `parquet:"inner,optional"`
}

// EmptyGroup is a struct with no fields - this creates an empty group in Parquet schema
type EmptyGroup struct{}

func main() {
	file, err := os.Create("empty_group_test.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a schema with the empty group
	schema := parquet.SchemaOf(Row{})

	writer := parquet.NewWriter(file, schema)

	// Write one row
	row := Row{
		ID:           1,
		Nested:       Nested{Inner: EmptyGroup{}},
		OptionalData: 100,
	}

	if err := writer.Write(&row); err != nil {
		log.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}

	log.Println("Created empty_group_test.parquet with schema:")
	log.Println("  message root {")
	log.Println("    optional int64 id;")
	log.Println("    optional group nested {")
	log.Println("      optional group inner {  // EMPTY GROUP: num_children=0, no type")
	log.Println("      }")
	log.Println("    }")
	log.Println("    optional int32 optional_data;")
	log.Println("  }")
}
