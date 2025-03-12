//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sorted_run.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

class SortedRun {
public:
	SortedRun(BufferManager &buffer_manager, const TupleDataLayout &key_layout, const TupleDataLayout &payload_layout);

public:
	//! Appends data to key/data collections
	void Sink(DataChunk &key, DataChunk &payload);
	//! Sorts the data (reorders payload and unpins data if external)
	void Finalize(bool external);

private:
	//! Key and payload collections (and append states)
	unique_ptr<TupleDataCollection> key_data;
	unique_ptr<TupleDataCollection> payload_data;
	TupleDataAppendState key_append_state;
	TupleDataAppendState payload_append_state;

	//! Whether this run has been finalized
	bool finalized;

	//! Min/max values (set after sort is complete)
	Value min;
	Value max;
};

} // namespace duckdb
