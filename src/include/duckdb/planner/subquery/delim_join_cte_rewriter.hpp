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

class DelimJoinCTEOptimizationDecision {
public:
	virtual ~DelimJoinCTEOptimizationDecision() = default;

	bool CanEvaluateAdditionalGroups() const {
		return can_evaluate_additional_groups;
	}
	bool CanEliminateEquivalentSourceDomain() const {
		return can_eliminate_equivalent_source_domain;
	}

protected:
	DelimJoinCTEOptimizationDecision(bool can_evaluate_additional_groups_p,
	                                 bool can_eliminate_equivalent_source_domain_p)
	    : can_evaluate_additional_groups(can_evaluate_additional_groups_p),
	      can_eliminate_equivalent_source_domain(can_eliminate_equivalent_source_domain_p) {
		D_ASSERT(!can_eliminate_equivalent_source_domain || can_evaluate_additional_groups);
	}

private:
	bool can_evaluate_additional_groups;
	bool can_eliminate_equivalent_source_domain;
};

struct DelimJoinCTEOptimizationAlternative {
	unique_ptr<LogicalOperator> plan;
	BindingReplacementGraph output_replacements;
};

enum class DelimJoinCTEOptimizationType : uint8_t { UNCHANGED, INLINED, ALTERNATIVE };

class DelimJoinCTEOptimizationResult {
public:
	static DelimJoinCTEOptimizationResult Unchanged() {
		return DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType::UNCHANGED);
	}

	static DelimJoinCTEOptimizationResult Inlined() {
		return DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType::INLINED);
	}

	static DelimJoinCTEOptimizationResult Alternative(unique_ptr<DelimJoinCTEOptimizationAlternative> alternative) {
		D_ASSERT(alternative);
		D_ASSERT(alternative->plan);
		return DelimJoinCTEOptimizationResult(std::move(alternative));
	}

	DelimJoinCTEOptimizationType Type() const {
		return type;
	}

	unique_ptr<DelimJoinCTEOptimizationAlternative> TakeAlternative() {
		D_ASSERT(type == DelimJoinCTEOptimizationType::ALTERNATIVE);
		D_ASSERT(alternative);
		return std::move(alternative);
	}

private:
	explicit DelimJoinCTEOptimizationResult(DelimJoinCTEOptimizationType type_p) : type(type_p) {
		D_ASSERT(type != DelimJoinCTEOptimizationType::ALTERNATIVE);
	}

	explicit DelimJoinCTEOptimizationResult(unique_ptr<DelimJoinCTEOptimizationAlternative> alternative_p)
	    : type(DelimJoinCTEOptimizationType::ALTERNATIVE), alternative(std::move(alternative_p)) {
	}

private:
	DelimJoinCTEOptimizationType type;
	unique_ptr<DelimJoinCTEOptimizationAlternative> alternative;
};

//! Supplies optional optimizer decisions to the policy-free CTE lowerer.
class DelimJoinCTEOptimization {
public:
	virtual ~DelimJoinCTEOptimization() = default;

	virtual void PreparePayload(Binder &binder, unique_ptr<LogicalOperator> &payload) = 0;
	virtual unique_ptr<DelimJoinCTEOptimizationDecision> Analyze(Binder &binder, LogicalOperator &rewrite_root,
	                                                             LogicalComparisonJoin &join, LogicalOperator &rhs,
	                                                             TableIndex domain_cte_index) = 0;
	//! Optionally build a candidate-source alternative after the decision's structural policy has been accepted.
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
	                                                optional_ptr<bool> plan_changed = nullptr,
	                                                bool null_rejecting_filter_above = false,
	                                                bool preserve_evidence_side = false,
	                                                bool under_preserved_evidence = false);
	BindingReplacementGraph RewriteDuplicateEliminatedJoin(unique_ptr<LogicalOperator> &plan,
	                                                       LogicalOperator &rewrite_root,
	                                                       bool null_rejecting_filter_above,
	                                                       bool preserve_evidence_side, bool under_preserved_evidence);
	bool TryInstallOptimizationAlternative(unique_ptr<LogicalOperator> &plan,
	                                       unique_ptr<DelimJoinCTEOptimizationAlternative> alternative,
	                                       const vector<ColumnBinding> &old_output_bindings,
	                                       const vector<LogicalType> &old_output_types,
	                                       BindingReplacementGraph &output_replacements);

private:
	Binder &binder;
	optional_ptr<DelimJoinCTEOptimization> optimization;
};

} // namespace duckdb
