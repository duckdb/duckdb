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
class LogicalComparisonJoin;
class LogicalCTE;

//! A materialized CTE definition and the delimiter-join stack depth at which it was registered
struct EnclosingCTE {
	EnclosingCTE(LogicalCTE &cte, idx_t delim_depth) : cte(cte), delim_depth(delim_depth) {
	}

	reference<LogicalCTE> cte;
	//! Number of delimiter joins enclosing the definition, used to resolve its delimiter scans
	idx_t delim_depth;
};

//! Rewrites fully decorrelated DelimJoins into materialized CTEs.
class DelimJoinCTERewriter {
public:
	static void Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan);

private:
	explicit DelimJoinCTERewriter(Binder &binder);

	void Rewrite(unique_ptr<LogicalOperator> &plan);
	BindingReplacementGraph RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                                                bool null_rejecting_filter_above = false,
	                                                bool preserve_evidence_side = false);
	BindingReplacementGraph MaterializeDelimJoinAsCTE(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                                                  bool null_rejecting_filter_above, bool preserve_evidence_side,
	                                                  bool preserve_nested_evidence_side);

private:
	Binder &binder;
	bool cte_deliminator_enabled;
	vector<TableIndex> generated_dedup_cte_indexes;
	set<TableIndex> marker_indexes;
	set<TableIndex> preserve_marker_indexes;
	set<TableIndex> preserve_nested_marker_indexes;
	//! Delim joins on whose RHS the current rewrite operates, used to resolve the domains of outer delim scans
	vector<reference<LogicalComparisonJoin>> enclosing_delim_joins;
	//! Materialized CTEs in whose consumer side the current rewrite operates, used to resolve CTE references
	vector<EnclosingCTE> enclosing_ctes;
};

} // namespace duckdb
