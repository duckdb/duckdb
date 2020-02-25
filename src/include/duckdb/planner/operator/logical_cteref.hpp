//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cteref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalChunkGet represents a scan operation from a ChunkCollection
class LogicalCTERef : public LogicalOperator {
public:
	LogicalCTERef(idx_t table_index, idx_t cte_index, vector<TypeId> types, vector<string> colnames)
	    : LogicalOperator(LogicalOperatorType::CTE_REF), table_index(table_index), cte_index(cte_index) {
		assert(types.size() > 0);
		chunk_types = types;
		bound_columns = colnames;
	}

	vector<string> bound_columns;
	//! The table index in the current bind context
	idx_t table_index;
	//! CTE index
	idx_t cte_index;
	//! The types of the chunk
	vector<TypeId> chunk_types;

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
