//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
public:
	LogicalProjection(index_t table_index, vector<unique_ptr<Expression>> select_list)
	    : LogicalOperator(LogicalOperatorType::PROJECTION, std::move(select_list)), table_index(table_index) {
	}

	index_t table_index;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
