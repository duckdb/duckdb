//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {
//! The JoinFilterPushdownOptimizer links comparison joins to data sources to enable dynamic execution-time filter pushdown
class JoinFilterPushdownOptimizer : public LogicalOperatorVisitor {
public:
	void VisitOperator(LogicalOperator &op) override;
};
} // namespace duckdb
