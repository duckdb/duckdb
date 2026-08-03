//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_optimizer.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_inliner.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
namespace duckdb {

class DuplicateEliminatedDomainDecision : public DelimJoinCTEOptimizationDecision {
public:
	DuplicateEliminatedDomainDecision(bool can_evaluate_additional_groups_p,
	                                  bool can_eliminate_equivalent_source_domain_p,
	                                  optional<DuplicateEliminatedDomainCandidate> candidate_p)
	    : DelimJoinCTEOptimizationDecision(can_evaluate_additional_groups_p, can_eliminate_equivalent_source_domain_p),
	      candidate(std::move(candidate_p)) {
	}

	optional<DuplicateEliminatedDomainCandidate> candidate;
};

bool DuplicateEliminatedDomainStrategy::Enabled(ClientContext &context) {
	return !Optimizer::OptimizerDisabled(context, OptimizerType::DUPLICATE_ELIMINATED_DOMAIN) &&
	       !Optimizer::OptimizerDisabled(context, OptimizerType::DELIMINATOR);
}

void DuplicateEliminatedDomainStrategy::PreparePayload(Binder &binder, unique_ptr<LogicalOperator> &payload) {
	if (Optimizer::OptimizerDisabled(binder.context, OptimizerType::FILTER_PUSHDOWN) ||
	    !DuplicateEliminatedDomainSafety::CanPreparePayload(binder.context, *payload)) {
		return;
	}
	Optimizer optimizer(binder, binder.context);
	FilterPushdown filter_pushdown(optimizer, false);
	payload = filter_pushdown.Rewrite(std::move(payload));
}

unique_ptr<DelimJoinCTEOptimizationDecision>
DuplicateEliminatedDomainStrategy::Analyze(Binder &binder, LogicalOperator &rewrite_root, LogicalComparisonJoin &join,
                                           LogicalOperator &rhs, TableIndex domain_cte_index) {
	DuplicateEliminatedDomainCTERegistry cte_registry(rewrite_root);
	auto can_evaluate_additional_groups = DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(
	    binder.context, rewrite_root, cte_registry, rhs, domain_cte_index);
	auto can_eliminate_equivalent_source_domain =
	    can_evaluate_additional_groups &&
	    DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(join, rhs, domain_cte_index);
	auto candidate =
	    DuplicateEliminatedDomainAnalyzer::FindBest(binder.context, cte_registry, join, can_evaluate_additional_groups);
	return make_uniq<DuplicateEliminatedDomainDecision>(can_evaluate_additional_groups,
	                                                    can_eliminate_equivalent_source_domain, std::move(candidate));
}

DelimJoinCTEOptimizationResult
DuplicateEliminatedDomainStrategy::TryOptimize(Binder &binder, unique_ptr<LogicalOperator> &join_op,
                                               TableIndex domain_cte_index, idx_t domain_ref_count,
                                               const DelimJoinCTEOptimizationDecision &decision_p) {
	auto &join = join_op->Cast<LogicalComparisonJoin>();
	auto &decision = static_cast<const DuplicateEliminatedDomainDecision &>(decision_p);
	if (!decision.candidate) {
		return DelimJoinCTEOptimizationResult::Unchanged();
	}
	auto &candidate = *decision.candidate;
	if (candidate.SourceCardinality() >= candidate.PayloadCardinality() ||
	    candidate.DomainCardinality() > candidate.PayloadDomainCardinality()) {
		return DelimJoinCTEOptimizationResult::Unchanged();
	}
	auto source = candidate.TryResolveSource(join.children[0]);
	if (!source) {
		return DelimJoinCTEOptimizationResult::Unchanged();
	}
	if (DuplicateEliminatedDomainInliner::TryInline(binder, join.children[1], *source, domain_cte_index,
	                                                domain_ref_count, candidate)) {
		return DelimJoinCTEOptimizationResult::Inlined();
	}
	auto alternative = DuplicateEliminatedDomainFactorer::TryFactor(binder, join_op, domain_cte_index, candidate);
	if (!alternative) {
		return DelimJoinCTEOptimizationResult::Unchanged();
	}
	return DelimJoinCTEOptimizationResult::Alternative(std::move(alternative));
}

unique_ptr<DelimJoinCTEOptimization> CreateDuplicateEliminatedDomainStrategy(ClientContext &context) {
	if (!DuplicateEliminatedDomainStrategy::Enabled(context)) {
		return nullptr;
	}
	return make_uniq<DuplicateEliminatedDomainStrategy>();
}

} // namespace duckdb
