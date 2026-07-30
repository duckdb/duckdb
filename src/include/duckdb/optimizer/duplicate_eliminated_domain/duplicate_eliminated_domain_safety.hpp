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
	//! Whether optional rewrites may remove the full-payload CTE without exposing expression evaluation.
	static bool CanOptimizePayload(const LogicalOperator &op);
	static bool CanEvaluateAdditionalGroups(const LogicalOperator &op, TableIndex domain_cte_index);
	//! Local factor eligibility, combined bottom-up by the candidate analyzer.
	static bool CanFactorOperator(const LogicalOperator &op);
	static bool CanDuplicateSource(const LogicalOperator &op);
};

} // namespace duckdb
