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
	BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> payload_types);
	virtual ~BaseAggregateHashTable() {
	}

protected:
	BufferManager &buffer_manager;
	//! A helper for managing offsets into the data buffers
	RowLayout layout;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;
};

} // namespace duckdb
