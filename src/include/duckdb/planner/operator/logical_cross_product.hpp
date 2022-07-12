//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
public:
	LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

public:
	vector<ColumnBinding> GetColumnBindings() override;

	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
