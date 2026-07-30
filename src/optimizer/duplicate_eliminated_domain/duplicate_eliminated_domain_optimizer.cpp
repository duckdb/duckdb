//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_inliner.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

namespace duckdb {

class DuplicateEliminatedDomainStrategy : public DelimJoinCTEOptimization {
public:
	bool CanEvaluateAdditionalGroups(const LogicalOperator &rhs, TableIndex domain_cte_index) override {
		return DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(rhs, domain_cte_index);
	}

	unique_ptr<FactoredDuplicateEliminatedDomain> TryOptimize(Binder &binder, unique_ptr<LogicalOperator> &join_op,
	                                                          TableIndex domain_cte_index, idx_t domain_ref_count,
	                                                          bool can_evaluate_additional_groups,
	                                                          bool &domain_inlined) override {
		auto &join = join_op->Cast<LogicalComparisonJoin>();
		auto candidate =
		    DuplicateEliminatedDomainAnalyzer::FindBest(binder.context, join, can_evaluate_additional_groups);
		if (!candidate) {
			return nullptr;
		}
		D_ASSERT(can_evaluate_additional_groups || candidate->Coverage() == DuplicateEliminatedDomainCoverage::EXACT);
		domain_inlined = DuplicateEliminatedDomainInliner::TryInline(binder, join.children[1], domain_cte_index,
		                                                             domain_ref_count, *candidate);
		if (domain_inlined) {
			return nullptr;
		}
		return DuplicateEliminatedDomainFactorer::TryFactor(binder, join_op, *candidate);
	}
};

DuplicateEliminatedDomainOptimizer::DuplicateEliminatedDomainOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

unique_ptr<LogicalOperator> DuplicateEliminatedDomainOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (!Settings::Get<DelimJoinAsCteSetting>(optimizer.context)) {
		return plan;
	}
	DelimJoinCTERewriter::NormalizeInputs(plan);
	if (optimizer.OptimizerDisabled(OptimizerType::DELIMINATOR)) {
		DelimJoinCTERewriter::Rewrite(optimizer.binder, plan);
	} else {
		DuplicateEliminatedDomainStrategy strategy;
		DelimJoinCTERewriter::Rewrite(optimizer.binder, plan, strategy);
	}
	return plan;
}

} // namespace duckdb
