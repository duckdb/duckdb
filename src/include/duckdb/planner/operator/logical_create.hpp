//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"

namespace duckdb {

//! LogicalCreate represents a CREATE operator
class LogicalCreate : public LogicalOperator {
public:
	LogicalCreate(LogicalOperatorType type, unique_ptr<BoundCreateInfo> info)
	    : LogicalOperator(type), info(move(info)) {
	}

	unique_ptr<BoundCreateInfo> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BOOL);
	}
};
} // namespace duckdb
