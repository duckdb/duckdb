//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/base_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {
class BufferManager;

class BaseAggregateHashTable {
public:
	BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
	                       vector<LogicalType> payload_types, vector<AggregateObject> aggregate_objects);
	virtual ~BaseAggregateHashTable() {
	}

protected:
	BufferManager &buffer_manager;
	//! A helper for managing offsets into the data buffers
	RowLayout layout;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;

	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;

protected:
	void CallDestructors(Vector &state_vector, idx_t count);
};

} // namespace duckdb
