//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

class JoinElimination : public LogicalOperatorVisitor {
public:
	explicit JoinElimination() {
	}

	// with specific condition we can eliminate a (left/right, semi, inner) join.
	// exemplify left/right join eliminaion condition:
	// 1. output can only have outer table columns
	// 2. join result cannot filter by inner table columns(ex. in where clause/ having clause)
	// 3. must ensure each outer row can match at most one inner table row, such as:
	//  1) inner table join condition is unique(ex. 1. join conditions have inner table's primary key 2. inner table join condition columns are all distinct)
	//  2) join result columns are all distinct
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

	unique_ptr<LogicalOperator> EliminateJoin(LogicalOperator &op);

private:
	unique_ptr<LogicalOperator> OptimizeChildren(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> TryEliminateJoin(unique_ptr<LogicalOperator> op);
	bool IsDistinctExpression(Expression &expr);

	idx_t inner_idx = 0;
	idx_t outer_idx = 0;

	column_binding_set_t column_references;
	column_binding_set_t distinct_column_references;
	// pushdown filter condition(ex in table scan operator),
	// if have outer table columns then cannot elimination
	bool inner_has_filter = false;
};
} // namespace duckdb
