//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

namespace duckdb {

class ClientContext;

//! Supplies optional duplicate-eliminated domain decisions at the decorrelation/lowering boundary.
class DuplicateEliminatedDomainStrategy : public DelimJoinCTEOptimization {
public:
	static bool Enabled(ClientContext &context);
	void PreparePayload(Binder &binder, unique_ptr<LogicalOperator> &payload) override;

	unique_ptr<DelimJoinCTEOptimizationDecision>
	Analyze(Binder &binder, LogicalOperator &rewrite_root, LogicalComparisonJoin &join, LogicalOperator &rhs,
	        TableIndex domain_cte_index) override;
	DelimJoinCTEOptimizationResult TryOptimize(Binder &binder, unique_ptr<LogicalOperator> &join,
	                                           TableIndex domain_cte_index, idx_t domain_ref_count,
	                                           const DelimJoinCTEOptimizationDecision &decision) override;
};

unique_ptr<DelimJoinCTEOptimization> CreateDuplicateEliminatedDomainStrategy(ClientContext &context);

} // namespace duckdb
