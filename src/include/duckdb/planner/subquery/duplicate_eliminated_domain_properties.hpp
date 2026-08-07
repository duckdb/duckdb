//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class LogicalOperator;
class LogicalComparisonJoin;

class DuplicateEliminatedDomainProperties {
public:
	//! Returns whether a subtree has a selection beyond generated column-equality join predicates.
	static bool HasNonJoinSelection(const LogicalOperator &op);
	//! Returns whether the RHS can produce at most one row for every SINGLE join key.
	static bool SingleJoinRHSIsDeduplicated(LogicalComparisonJoin &join, LogicalOperator &root);
};

} // namespace duckdb
