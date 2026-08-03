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

class GeneratedDomainJoinRewriter;

class DelimJoinCTEOptimizationDecision {
public:
	virtual ~DelimJoinCTEOptimizationDecision() = default;

	bool CanEvaluateAdditionalGroups() const {
		return can_evaluate_additional_groups;
	}
	bool HasCandidate() const {
		return has_candidate;
	}
	bool CandidateHasSelection() const {
		D_ASSERT(has_candidate);
		return candidate_has_selection;
	}

protected:
	DelimJoinCTEOptimizationDecision(bool can_evaluate_additional_groups_p, bool has_candidate_p,
	                                bool candidate_has_selection_p)
	    : can_evaluate_additional_groups(can_evaluate_additional_groups_p), has_candidate(has_candidate_p),
	      candidate_has_selection(candidate_has_selection_p) {
	}

private:
	bool can_evaluate_additional_groups;
	bool has_candidate;
	bool candidate_has_selection;
};

struct FactoredDuplicateEliminatedDomain {
	TableIndex cte_index;
	Identifier cte_name;
	idx_t column_count;
	unique_ptr<LogicalOperator> source;
	unique_ptr<LogicalOperator> domain;
	unique_ptr<LogicalOperator> child;
	BindingReplacementGraph output_replacements;
};

enum class DelimJoinCTEOptimizationType : uint8_t { UNCHANGED, INLINED, FACTORED };

class DelimJoinCTEOptimizationResult {
public:
	static DelimJoinCTEOptimizationResult Unchanged() {
		return DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType::UNCHANGED);
	}

	static DelimJoinCTEOptimizationResult Inlined() {
		return DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType::INLINED);
	}

	static DelimJoinCTEOptimizationResult Factored(unique_ptr<FactoredDuplicateEliminatedDomain> factored_domain) {
		D_ASSERT(factored_domain);
		return DelimJoinCTEOptimizationResult(std::move(factored_domain));
	}

	DelimJoinCTEOptimizationType Type() const {
		return type;
	}

	unique_ptr<FactoredDuplicateEliminatedDomain> TakeFactoredDomain() {
		D_ASSERT(type == DelimJoinCTEOptimizationType::FACTORED);
		D_ASSERT(factored_domain);
		return std::move(factored_domain);
	}

private:
	explicit DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType type_p) : type(type_p) {
		D_ASSERT(type != DelimJoinCTEOptimizationType::FACTORED);
	}

	explicit DelimJoinCTEOptimizationResult(unique_ptr<FactoredDuplicateEliminatedDomain> factored_domain_p)
	    : type(DelimJoinCTEOptimizationType::FACTORED), factored_domain(std::move(factored_domain_p)) {
	}

private:
	DelimJoinCTEOptimizationType type;
	unique_ptr<FactoredDuplicateEliminatedDomain> factored_domain;
};

//! Supplies optional optimizer decisions to the policy-free CTE lowerer.
class DelimJoinCTEOptimization {
public:
	virtual ~DelimJoinCTEOptimization() = default;

	virtual void PreparePayload(Binder &binder, unique_ptr<LogicalOperator> &payload) = 0;
	virtual unique_ptr<DelimJoinCTEOptimizationDecision>
	Analyze(Binder &binder, LogicalOperator &rewrite_root, LogicalComparisonJoin &join, LogicalOperator &rhs,
	        TableIndex domain_cte_index) = 0;
	virtual DelimJoinCTEOptimizationResult TryOptimize(Binder &binder, unique_ptr<LogicalOperator> &join,
	                                                   TableIndex domain_cte_index, idx_t domain_ref_count,
	                                                   const DelimJoinCTEOptimizationDecision &decision) = 0;
};

//! Rewrites fully decorrelated DelimJoins into materialized CTEs.
class DelimJoinCTERewriter {
public:
	//! Lower fully decorrelated DelimJoins. Optimizer policy is supplied explicitly.
	static void Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan,
	                    optional_ptr<DelimJoinCTEOptimization> optimization = nullptr);

private:
	DelimJoinCTERewriter(Binder &binder, optional_ptr<DelimJoinCTEOptimization> optimization);

	void RewriteInternal(unique_ptr<LogicalOperator> &plan);
	BindingReplacementGraph RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                                                GeneratedDomainJoinRewriter &generated_domain_rewriter,
	                                                bool &plan_changed, bool null_rejecting_filter_above = false,
	                                                bool preserve_evidence_side = false,
	                                                bool under_preserved_evidence = false);
	BindingReplacementGraph RewriteDuplicateEliminatedJoin(unique_ptr<LogicalOperator> &plan,
	                                                       LogicalOperator &rewrite_root,
	                                                       GeneratedDomainJoinRewriter &generated_domain_rewriter,
	                                                       bool null_rejecting_filter_above,
	                                                       bool preserve_evidence_side, bool under_preserved_evidence);

private:
	Binder &binder;
	optional_ptr<DelimJoinCTEOptimization> optimization;
};

} // namespace duckdb
