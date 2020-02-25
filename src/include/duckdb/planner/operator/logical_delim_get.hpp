//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_delim_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDelimGet represents a duplicate eliminated scan belonging to a DelimJoin
class LogicalDelimGet : public LogicalOperator {
public:
	LogicalDelimGet(idx_t table_index, vector<TypeId> types)
	    : LogicalOperator(LogicalOperatorType::DELIM_GET), table_index(table_index) {
		assert(types.size() > 0);
		chunk_types = types;
	}

	//! The table index in the current bind context
	idx_t table_index;
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
