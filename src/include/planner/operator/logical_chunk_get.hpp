//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_chunk_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalChunkGet : public LogicalOperator {
public:
	LogicalChunkGet(size_t table_index, vector<TypeId> types)
	    : LogicalOperator(LogicalOperatorType::CHUNK_GET), table_index(table_index) {
		assert(types.size() > 0);
		chunk_types = types;
	}

	vector<string> GetNames() override {
		vector<string> names;
		for (size_t i = 0; i < chunk_types.size(); i++) {
			names.push_back(std::to_string(i));
		}
		return names;
	}

	//! The table index in the current bind context
	size_t table_index;
	//! The types of the chunk
	vector<TypeId> chunk_types;

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};
} // namespace duckdb
