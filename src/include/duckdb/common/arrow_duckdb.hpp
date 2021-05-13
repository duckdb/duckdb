//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow_duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "arrow.hpp"
#include <bitset>
//! Here we have the internal duckdb classes that interact with Arrow's Internal Header (i.e., duckdb/commons/arrow.hpp)
namespace duckdb {
class ArrowSchemaDuck {
public:
	ArrowSchema arrow_schema;

	ArrowSchemaDuck() {
		arrow_schema.release = nullptr;
	}

	~ArrowSchemaDuck();
};
class ArrowArrayDuck {
public:
	ArrowArray arrow_array;
	ArrowArrayDuck() {
		arrow_array.length = 0;
		arrow_array.release = nullptr;
	}
	~ArrowArrayDuck();
};

class ArrowArrayStreamDuck {
public:
	ArrowArrayStream arrow_array_stream;
	uint64_t number_of_batches = 0;
	uint64_t first_batch_size = 0;
	uint64_t last_batch_size = 0;

	void GetSchema(ArrowSchemaDuck &schema);

	unique_ptr<ArrowArrayDuck> GetNextChunk();

	const char *GetError();

	~ArrowArrayStreamDuck();
	ArrowArrayStreamDuck() {
		arrow_array_stream.release = nullptr;
	}
};

} // namespace duckdb
