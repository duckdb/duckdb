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

struct FactoredDuplicateEliminatedDomain {
	TableIndex cte_index;
	Identifier cte_name;
	idx_t column_count;
	unique_ptr<LogicalOperator> source;
	unique_ptr<LogicalOperator> domain;
	BindingReplacementGraph output_replacements;
};

//! Supplies optional optimizer decisions to the policy-free CTE lowerer.
class DelimJoinCTEOptimization {
public:
	virtual ~DelimJoinCTEOptimization() = default;

	virtual bool CanEvaluateAdditionalGroups(const LogicalOperator &rhs, TableIndex domain_cte_index) = 0;
	virtual unique_ptr<FactoredDuplicateEliminatedDomain>
	TryOptimize(Binder &binder, unique_ptr<LogicalOperator> &join, TableIndex domain_cte_index, idx_t domain_ref_count,
	            bool can_evaluate_additional_groups, bool &domain_inlined) = 0;
};

//! Rewrites fully decorrelated DelimJoins into materialized CTEs.
class DelimJoinCTERewriter {
public:
	//! Move filters on the payload side to their canonical pre-lowering location.
	static void NormalizeInputs(unique_ptr<LogicalOperator> &plan);
	//! Lower fully decorrelated DelimJoins. Optimizer policy is supplied explicitly.
	static void Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan,
	                    optional_ptr<DelimJoinCTEOptimization> optimization = nullptr);

private:
	DelimJoinCTERewriter(Binder &binder, optional_ptr<DelimJoinCTEOptimization> optimization);

	void RewriteInternal(unique_ptr<LogicalOperator> &plan);
	BindingReplacementGraph RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
	                                                bool null_rejecting_filter_above = false,
	                                                bool preserve_evidence_side = false);
	BindingReplacementGraph RewriteDuplicateEliminatedJoin(unique_ptr<LogicalOperator> &plan,
	                                                       LogicalOperator &rewrite_root,
	                                                       bool null_rejecting_filter_above,
	                                                       bool preserve_evidence_side);

private:
	Binder &binder;
	optional_ptr<DelimJoinCTEOptimization> optimization;
	unordered_map<TableIndex, DuplicateEliminatedDomainExpansion> generated_dedup_ctes;
};

} // namespace duckdb
