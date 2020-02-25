//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_chunk_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalChunkGet represents a scan operation from a ChunkCollection
class LogicalChunkGet : public LogicalOperator {
public:
	LogicalChunkGet(idx_t table_index, vector<TypeId> types, unique_ptr<ChunkCollection> collection)
	    : LogicalOperator(LogicalOperatorType::CHUNK_GET), table_index(table_index), collection(move(collection)) {
		assert(types.size() > 0);
		chunk_types = types;
	}

	//! The table index in the current bind context
	idx_t table_index;
	//! The types of the chunk
	vector<TypeId> chunk_types;
	//! The chunk collection to scan
	unique_ptr<ChunkCollection> collection;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, chunk_types.size());
	}

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};
} // namespace duckdb
