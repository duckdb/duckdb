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
	//! Returns whether a subtree contains a predicate that can reduce a duplicate-eliminated domain.
	static bool HasSelection(const LogicalOperator &op);
};

} // namespace duckdb
