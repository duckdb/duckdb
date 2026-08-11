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
class ClientContext;
class DuplicateEliminatedDomainCTERegistry;

//! Conservative eligibility checks for duplicate-eliminated domain rewrites.
//! Unsupported operators and expressions always decline the optimization.
class DuplicateEliminatedDomainSafety {
public:
	//! Whether local filter pushdown can prepare this payload without changing its public layout.
	static bool CanPreparePayload(ClientContext &context, const LogicalOperator &op);
	static bool CanEvaluateAdditionalGroups(ClientContext &context, LogicalOperator &rewrite_root,
	                                        const DuplicateEliminatedDomainCTERegistry &cte_registry,
	                                        LogicalOperator &op, TableIndex domain_cte_index);
	//! Local factor eligibility, combined bottom-up by the candidate analyzer.
	static bool CanFactorOperator(ClientContext &context, const LogicalOperator &op);
	static bool CanDuplicateSource(ClientContext &context, LogicalOperator &op);
};

} // namespace duckdb
