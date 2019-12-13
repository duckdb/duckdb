//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_binding_resolver.hpp"

namespace duckdb {

//! LogicalEmptyResult returns an empty result. This is created by the optimizer if it can reason that ceratin parts of
//! the tree will always return an empty result.
class LogicalEmptyResult : public LogicalOperator {
public:
	LogicalEmptyResult(unique_ptr<LogicalOperator> op);

	vector<TypeId> return_types;
	//! The tables that would be bound at this location (if the subtree was not optimized away)
	vector<BoundTable> bound_tables;

protected:
	void ResolveTypes() override {
		this->types = return_types;
	}
};
} // namespace duckdb
