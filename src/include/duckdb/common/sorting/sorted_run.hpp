//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sorted_run.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

class BufferManager;
class DataChunk;
class TupleDataCollection;
class TupleDataLayout;

class SortedRun {
public:
	SortedRun(BufferManager &buffer_manager, const TupleDataLayout &key_layout, const TupleDataLayout &payload_layout);

	~SortedRun();

public:
	//! Appends data to key/data collections
	void Sink(DataChunk &key, DataChunk &payload);
	//! Sorts the data (physically reorder data if external)
	void Finalize(bool external);
	//! Number of tuples
	idx_t Count() const;
	//! Size of this sorted run
	idx_t SizeInBytes() const;

public:
	//! Key and payload collections (and associated append states)
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
