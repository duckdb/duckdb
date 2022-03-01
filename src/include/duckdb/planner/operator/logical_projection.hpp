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
	LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list);

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void set_table_index(idx_t set_index);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
