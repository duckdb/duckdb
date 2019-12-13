//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::JOIN);

	// Gets the set of table references that are reachable from this node
	static void GetTableReferences(LogicalOperator &op, unordered_set<index_t> &bindings);
	static void GetExpressionBindings(Expression &expr, unordered_set<index_t> &bindings);

	//! The type of the join (INNER, OUTER, etc...)
	JoinType type;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
