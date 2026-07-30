//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/table_index.hpp"

namespace duckdb {

class LogicalOperator;

//! Proves whether duplicate-eliminated domain rewrites can evaluate or copy a logical subtree.
class DuplicateEliminatedDomainSafety {
public:
	//! Returns whether evaluating the plan for additional domain groups is unobservable.
	static bool CanEvaluateAdditionalGroups(const LogicalOperator &op, TableIndex domain_cte_index);
	//! Returns whether the source can be copied without changing its observable behavior.
	static bool CanDuplicateSource(const LogicalOperator &op);
};

} // namespace duckdb
