#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parallel/pipeline_dependency_set.hpp"

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
	                                           const vector<Branch> &branches,
	                                           shared_ptr<PipelineDependencySet> dep_set);

	//! Build LEFT join: inner matches plus LHS anti-join chain for unmatched rows
	unique_ptr<LogicalOperator> BuildLeftJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches,
	                                          shared_ptr<PipelineDependencySet> dep_set);

	//! Build RIGHT join: same as LEFT with sides swapped
	unique_ptr<LogicalOperator> BuildRightJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                           const vector<Branch> &branches,
	                                           shared_ptr<PipelineDependencySet> dep_set);

	//! Build FULL join: inner matches + probe-side anti chain + unmatched build rows
	unique_ptr<LogicalOperator> BuildFullJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches,
	                                          shared_ptr<PipelineDependencySet> dep_set);

	//! Build SEMI join: chained MARK joins, filter at end if any marker is true
	unique_ptr<LogicalOperator> BuildSemiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                          const vector<Branch> &branches);

	//! Build ANTI join: chained anti joins
	unique_ptr<LogicalOperator> BuildAntiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                             const vector<Branch> &branches;

    //! Builds a single hash join branch for a specific equality predicate
	unique_ptr<LogicalOperator> BuildHashJoinBranch(
		const CTEInfo &left_cte,
		const CTEInfo &right_cte,
		const Branch &branch,
		JoinType join_type,
		const vector<unique_ptr<Expression>> *exclusion_predicates = nullptr,
		TableIndex left_ref = TableIndex(DConstants::INVALID_INDEX),
		TableIndex right_ref = TableIndex(DConstants::INVALID_INDEX),
		shared_ptr<PipelineDependencySet> dep_set = nullptr);

    //! Builds exclusion predicates for branch N (rechecks all previous branch predicates)
    //! we recheck earlier predicates to exclude already-matched pairs
    vector<unique_ptr<Expression>> BuildExclusionPredicates(
        const CTEInfo &left_cte,
        const CTEInfo &right_cte,
        const vector<Branch> &branches,
        idx_t current_branch_idx,
        TableIndex left_ref_idx,
        TableIndex right_ref_idx);

    //! Builds the anti-join chain for finding unmatched probe-side rows (LEFT/FULL joins)
    unique_ptr<LogicalOperator> BuildAntiJoinChain(
        const CTEInfo &probe_cte,
        const CTEInfo &build_cte,
        const vector<Branch> &branches,
        TableIndex probe_ref_idx,
        const string &build_alias_prefix);

    //! Builds MARK join chain for SEMI joins
    unique_ptr<LogicalOperator> BuildMarkJoinChain(
        const CTEInfo &left_cte,
        const CTEInfo &right_cte,
        const vector<Branch> &branches);

    //! For FULL joins: collects matched build-side row IDs using aggregation
    unique_ptr<LogicalOperator> BuildUnmatchedBuildSide(
        const CTEInfo &build_cte,
        const CTEInfo &probe_cte,
        const vector<Branch> &branches,
        TableIndex build_ref_idx,
        TableIndex probe_ref_idx,
        ColumnBinding &matched_build_ids_binding,
        shared_ptr<PipelineDependencySet> unmatched_rhs_dep_set);


    //! Normalizes rewrite output to original binding order
    unique_ptr<LogicalOperator> NormalizeOutput(
        unique_ptr<LogicalOperator> plan,
        const vector<ColumnBinding> &orig_bindings,
        const vector<LogicalType> &orig_types,
        const CTEInfo &left_cte,
        const CTEInfo &right_cte,
        JoinType join_type);

    idx_t GetCTEColumnIndex(const CTEInfo &cte, ColumnBinding original_binding);


    //! Creates a shared barrier object for synchronizing build/probe phases
    shared_ptr<struct CTEFanoutBarrier> CreateBarrier();

    //! Sets the barrier on a comparison join operator
    void SetBarrierOnJoin(LogicalComparisonJoin &join, shared_ptr<CTEFanoutBarrier> barrier);

    ClientContext &context;
    Binder &binder;
    ColumnBindingReplacer replacer;
};

//! Barrier object for synchronizing streaming CTE fan-out execution.
//! Shared across all hash join operators that consume from the same CTE.
struct CTEFanoutBarrier {
	mutex lock;
	atomic<idx_t> pending_builds {0};
	atomic<bool> builds_complete {false};
	std::condition_variable builds_complete_cv;

	//! Called by each build pipeline when it starts building
	void StartBuild();

	//! Called by each build pipeline when it finishes building
	void FinalizeBuild();

	//! Called by probe pipelines to wait for all builds to complete
	void WaitForBuild();

	//! Check if all builds have completed
	bool IsBuildComplete() const;
};

} // namespace duckdb
