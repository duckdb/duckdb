//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

namespace duckdb {

DuplicateEliminatedDomainOptimizer::DuplicateEliminatedDomainOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> DuplicateEliminatedDomainOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (!Settings::Get<DelimJoinAsCteSetting>(optimizer.context)) {
		return plan;
	}
	auto rewritten = optimizer.OptimizerDisabled(OptimizerType::DELIMINATOR)
	                     ? DelimJoinCTERewriter::RewriteForExecution(optimizer.binder, plan)
	                     : DelimJoinCTERewriter::RewriteAndOptimize(optimizer.binder, plan);
	if (!rewritten) {
		return plan;
	}

	// This constituent cleanup handles ordinary MARK joins exposed after the scheduled filter-pushdown pass.
	FilterPushdown filter_pushdown(optimizer);
	unordered_set<TableIndex> top_bindings;
	filter_pushdown.CheckMarkToSemi(*plan, top_bindings);
	return filter_pushdown.Rewrite(std::move(plan));
}

} // namespace duckdb
