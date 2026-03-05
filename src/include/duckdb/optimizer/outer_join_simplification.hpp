//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/outer_join_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

//! Simplifies FULL OUTER -> LEFT/RIGHT OUTER -> INNER if NULLs are filtered anyway
class OuterJoinSimplification : public LogicalOperatorVisitor {
public:
	OuterJoinSimplification();

public:
	void VisitOperator(LogicalOperator &op) override;

private:
	void HandleExpression(const Expression &expr);

private:
	//! Column bindings that have their NULL values filtered
	column_binding_set_t null_filtered;
};

} // namespace duckdb
