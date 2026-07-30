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

class DuplicateEliminatedDomainProperties {
public:
	//! Returns whether a subtree has a selection beyond generated column-equality join predicates.
	static bool HasNonJoinSelection(const LogicalOperator &op);
};

} // namespace duckdb
