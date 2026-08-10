#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class DisjunctiveJoinRewriter {
public:
	explicit DisjunctiveJoinRewriter(ClientContext &context, Binder &binder);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

	static unique_ptr<Expression> ColRef(ColumnBinding binding, const LogicalType &type, const string &alias = "");

private:
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op);

	//! Represents a single equality predicate branch from an OR condition
	struct Branch {
		unique_ptr<Expression> left_expr;
		unique_ptr<Expression> right_expr;
		LogicalType left_type;
		LogicalType right_type;
	};

	//! Information about a CTE
	struct CTEInfo {
		TableIndex table_index;
		vector<LogicalType> output_types;
		vector<ColumnBinding> output_bindings;
		vector<ColumnBinding> original_bindings;
	};

	//! Checks if join has OR of equalities we can rewrite
	bool ShouldRewrite(const LogicalAnyJoin &join, const unordered_set<TableIndex> &left_tables,
	                   const unordered_set<TableIndex> &right_tables, vector<Branch> &out_branches) const;

	//! Flattens OR tree into list of equality branches
	bool FlattenOR(const Expression &expr, const unordered_set<TableIndex> &left_tables,
	               const unordered_set<TableIndex> &right_tables, vector<Branch> &out) const;

	TableIndex NewTableIndex();

	unique_ptr<LogicalOperator> MakeCTERef(const CTEInfo &cte, TableIndex ref_idx) const;

	//! Build INNER join: disjoint hash-join branches combined with UNION ALL
	unique_ptr<LogicalOperator> BuildInnerJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                           const vector<Branch> &branches);

	//! Build LEFT join: inner matches plus LHS anti-join chain for unmatched rows
	unique_ptr<LogicalOperator> BuildLeftJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches);

	//! Build RIGHT join: same as LEFT with sides swapped
	unique_ptr<LogicalOperator> BuildRightJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                           const vector<Branch> &branches);

	//! Build FULL join: inner matches + probe-side anti chain + build-side anti chain
	unique_ptr<LogicalOperator> BuildFullJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches);

	//! Build SEMI join: chained MARK joins, filter at end if any marker is true
	unique_ptr<LogicalOperator> BuildSemiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches);

	//! Build ANTI join: chained anti joins
	unique_ptr<LogicalOperator> BuildAntiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches);

	//! Creates the matching INNER join branches and projects original columns
	vector<unique_ptr<LogicalOperator>> BuildMatchedBranches(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                                         const vector<Branch> &branches);

	//! Creates the anti-join chain for unmatched rows and NULL-pads the build side
	unique_ptr<LogicalOperator> BuildUnmatchedPart(const CTEInfo &probe_cte, const CTEInfo &build_cte,
	                                               const vector<Branch> &branches, bool put_probe_first = true);

	//! Wraps multiple operator plans into a UNION ALL
	unique_ptr<LogicalOperator> BuildUnionAll(vector<unique_ptr<LogicalOperator>> children, idx_t total_columns);

	//! Swaps left/right expressions in branches
	vector<Branch> SwapBranches(const vector<Branch> &branches);

	//! Remaps branch expressions to current CTE references
	void RemapBranchExpressions(const CTEInfo &left_cte, const CTEInfo &right_cte, TableIndex left_ref,
	                            TableIndex right_ref, unique_ptr<Expression> &left_expr,
	                            unique_ptr<Expression> &right_expr);

	//! Builds a single hash join branch for a specific equality predicate
	unique_ptr<LogicalOperator>
	BuildHashJoinBranch(const CTEInfo &left_cte, const CTEInfo &right_cte, const Branch &branch, JoinType join_type,
	                    const vector<unique_ptr<Expression>> *exclusion_predicates = nullptr,
	                    TableIndex left_ref = TableIndex(DConstants::INVALID_INDEX),
	                    TableIndex right_ref = TableIndex(DConstants::INVALID_INDEX));

	//! Builds exclusion predicates for branch N (rechecks all previous branch predicates)
	//! we recheck earlier predicates to exclude already-matched pairs
	vector<unique_ptr<Expression>> BuildExclusionPredicates(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                                        const vector<Branch> &branches, idx_t current_branch_idx,
	                                                        TableIndex left_ref_idx, TableIndex right_ref_idx);

	//! Builds the anti-join chain for finding unmatched probe-side rows (LEFT/FULL joins)
	unique_ptr<LogicalOperator> BuildAntiJoinChain(const CTEInfo &probe_cte, const CTEInfo &build_cte,
	                                               const vector<Branch> &branches, TableIndex probe_ref_idx);

	//! Normalizes rewrite output to original binding order
	unique_ptr<LogicalOperator> NormalizeOutput(unique_ptr<LogicalOperator> plan,
	                                            const vector<ColumnBinding> &orig_bindings,
	                                            const vector<LogicalType> &orig_types, const CTEInfo &left_cte,
	                                            const CTEInfo &right_cte, JoinType join_type);

	idx_t GetCTEColumnIndex(const CTEInfo &cte, ColumnBinding original_binding);

	ClientContext &context;
	Binder &binder;
	ColumnBindingReplacer replacer;
};

} // namespace duckdb
