//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

#include <unordered_set>

namespace duckdb {

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::JOIN);

	vector<string> GetNames() override;

	// Gets the set of table references that are reachable from this node
	static void GetTableReferences(LogicalOperator &op, unordered_set<size_t> &bindings);

	//! The type of the join (INNER, OUTER, etc...)
	JoinType type;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
