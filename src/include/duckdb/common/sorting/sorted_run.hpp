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
	SortedRun(BufferManager &buffer_manager, shared_ptr<TupleDataLayout> key_layout,
	          shared_ptr<TupleDataLayout> payload_layout);

	~SortedRun();

public:
	//! Appends data to key/data collections
	void Sink(DataChunk &key, DataChunk &payload);
	//! Sorts the data (physically reorder data if external)
	void Finalize(bool external);
	//! Destroy data between these tuple indices
	void DestroyData(idx_t begin_idx, idx_t end_idx);
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
};

} // namespace duckdb
