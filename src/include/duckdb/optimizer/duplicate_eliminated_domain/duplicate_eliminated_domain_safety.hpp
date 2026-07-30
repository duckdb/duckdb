//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/table_index.hpp"

namespace duckdb {

class LogicalOperator;

//! Conservative eligibility checks for duplicate-eliminated domain rewrites.
//! Unsupported operators and opaque function bind data always decline the optimization.
class DuplicateEliminatedDomainSafety {
public:
	static bool CanEvaluateAdditionalGroups(const LogicalOperator &op, TableIndex domain_cte_index);
	static bool CanFactorSource(const LogicalOperator &op);
	static bool CanDuplicateSource(const LogicalOperator &op);
};

} // namespace duckdb
