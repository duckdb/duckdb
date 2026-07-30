//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class LogicalOperator;

//! Properties used when choosing between duplicate-eliminated domain implementations.
class DuplicateEliminatedDomainProperties {
public:
	//! Returns whether a subtree contains a predicate that can reduce its input domain.
	static bool HasSelection(const LogicalOperator &op);
};

} // namespace duckdb
