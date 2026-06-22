//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/delim_join_cte_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Rewrites fully decorrelated DelimJoins into materialized CTEs.
class DelimJoinCTERewriter {
public:
	static void Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan);

private:
	explicit DelimJoinCTERewriter(Binder &binder);

	void Rewrite(unique_ptr<LogicalOperator> &plan);
	void RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                             bool null_rejecting_filter_above = false, bool preserve_evidence_side = false);
	void MaterializeDelimJoinAsCTE(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                               bool null_rejecting_filter_above, bool preserve_evidence_side);

private:
	Binder &binder;
	bool cte_deliminator_enabled;
	vector<TableIndex> generated_dedup_cte_indexes;
};

} // namespace duckdb
