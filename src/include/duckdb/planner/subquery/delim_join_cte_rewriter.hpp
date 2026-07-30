//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/delim_join_cte_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

enum class DuplicateEliminatedDomainExpansion : uint8_t { UNSAFE, SAFE };

//! Rewrites fully decorrelated DelimJoins into materialized CTEs.
class DelimJoinCTERewriter {
public:
	//! Lower fully decorrelated DelimJoins to the baseline materialized-CTE representation.
	static bool RewriteForExecution(Binder &binder, unique_ptr<LogicalOperator> &plan);
	//! Lower DelimJoins and optimize their generated duplicate-eliminated domains.
	static bool RewriteAndOptimize(Binder &binder, unique_ptr<LogicalOperator> &plan);

private:
	enum class DuplicateEliminatedJoinRewriteMode : uint8_t { EXECUTION, OPTIMIZED };

	DelimJoinCTERewriter(Binder &binder, DuplicateEliminatedJoinRewriteMode mode);

	bool Rewrite(unique_ptr<LogicalOperator> &plan);
	BindingReplacementGraph RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                                                bool null_rejecting_filter_above = false,
	                                                bool preserve_evidence_side = false);
	BindingReplacementGraph RewriteDuplicateEliminatedJoin(unique_ptr<LogicalOperator> &plan,
	                                                       LogicalOperator &rewrite_root,
	                                                       bool null_rejecting_filter_above,
	                                                       bool preserve_evidence_side);

private:
	Binder &binder;
	DuplicateEliminatedJoinRewriteMode mode;
	bool rewritten_delim_join = false;
	unordered_map<TableIndex, DuplicateEliminatedDomainExpansion> generated_dedup_ctes;
};

} // namespace duckdb
