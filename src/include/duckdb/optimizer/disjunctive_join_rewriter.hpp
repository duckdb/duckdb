#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class DisjunctiveJoinRewriter {
public:
	explicit DisjunctiveJoinRewriter(ClientContext &context, Binder &binder);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

	static unique_ptr<Expression> ColRef(ColumnBinding binding, const LogicalType &type, const string &alias = "");

private:
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op);

	struct Branch {
		unique_ptr<Expression> left_expr;
		unique_ptr<Expression> right_expr;
		LogicalType left_type;
		LogicalType right_type;
	};

	struct CTEInfo {
		TableIndex table_index;
		vector<LogicalType> output_types;
		vector<ColumnBinding> output_bindings;
		vector<ColumnBinding> original_bindings;
	};

	struct RowIDResult {
		unique_ptr<LogicalOperator> plan;
		ColumnBinding rowid_col;
		vector<LogicalType> all_types;
		vector<ColumnBinding> all_bindings;
		bool used_physical_rowid;
	};

	struct KeyTableResult {
		unique_ptr<LogicalOperator> plan;
		ColumnBinding rowid_col;
		ColumnBinding tag_col;
		ColumnBinding key_col;
	};

	struct MatchCTEResult {
		unique_ptr<LogicalOperator> plan;
		vector<LogicalType> output_types;
		vector<ColumnBinding> output_bindings;
	};

	static constexpr idx_t TINY_TABLE_ROW_THRESHOLD = 2048;

	static constexpr double H2_MEMORY_FRACTION = 0.50;

	static constexpr idx_t COLUMN_COUNT_PENALTY = 2;

	//! checks if join has OR of equalities we can rewrite
	bool ShouldRewrite(const LogicalAnyJoin &join, const unordered_set<TableIndex> &left_tables,
	                   const unordered_set<TableIndex> &right_tables, vector<Branch> &out_branches) const;

	//! flattens OR tree into list of equality branches
	bool FlattenOR(const Expression &expr, const unordered_set<TableIndex> &left_tables,
	               const unordered_set<TableIndex> &right_tables, vector<Branch> &out) const;

	bool HasPhysicalRowID(const LogicalOperator &op) const;

	idx_t GetBaseCardinality(LogicalOperator &op) const;

	idx_t GetConfiguredMemoryLimit() const;

	static double ComputeMaterializationCost(vector<LogicalType> types, idx_t cardinality);

	bool PassHeuristics(const LogicalAnyJoin &join, const vector<Branch> &branches) const;

	bool TryInjectPhysicalRowID(LogicalOperator &child, const string &alias, ColumnBinding &out_rowid_binding);

	RowIDResult InjectRowNumRowID(unique_ptr<LogicalOperator> child, const string &alias);

	RowIDResult InjectRowID(unique_ptr<LogicalOperator> child, const string &alias);

	TableIndex NewTableIndex();

	unique_ptr<LogicalOperator> MakeCTERef(const CTEInfo &cte, TableIndex ref_idx) const;

	//! builds match cte from all branches (full rewrite path)
	MatchCTEResult BuildMatchCTE(const CTEInfo &left_cte, const CTEInfo &right_cte, ColumnBinding left_rowid,
	                             ColumnBinding right_rowid, const vector<Branch> &branches);

	unique_ptr<LogicalOperator> BuildInner(const CTEInfo &match_cte, const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                       ColumnBinding left_rowid, ColumnBinding right_rowid);

	unique_ptr<LogicalOperator> BuildLeft(const CTEInfo &match_cte, const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                      ColumnBinding left_rowid, ColumnBinding right_rowid);

	unique_ptr<LogicalOperator> BuildRight(const CTEInfo &match_cte, const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                       ColumnBinding left_rowid, ColumnBinding right_rowid);

	unique_ptr<LogicalOperator> BuildFull(const CTEInfo &match_cte, const CTEInfo &left_cte, const CTEInfo &right_cte,
	                                      ColumnBinding left_rowid, ColumnBinding right_rowid);

	unique_ptr<LogicalOperator> BuildSemi(const CTEInfo &match_cte, const CTEInfo &left_cte, ColumnBinding left_rowid);

	unique_ptr<LogicalOperator> BuildAnti(const CTEInfo &match_cte, const CTEInfo &left_cte, ColumnBinding left_rowid);

	unique_ptr<LogicalOperator> BuildTwoSidedJoin(const CTEInfo &match_cte, const CTEInfo &left_cte,
	                                              const CTEInfo &right_cte, ColumnBinding left_rowid,
	                                              ColumnBinding right_rowid, JoinType first_join, JoinType second_join,
	                                              bool swap_build_order = false);

	unique_ptr<LogicalOperator> BuildOneSidedJoin(const CTEInfo &match_cte, const CTEInfo &left_cte,
	                                              ColumnBinding left_rowid, JoinType join_type);

	//! normalizes full rewrite output to original binding order
	unique_ptr<LogicalOperator> NormaliseOutput(unique_ptr<LogicalOperator> epilogue,
	                                            const vector<ColumnBinding> &orig_bindings,
	                                            const vector<LogicalType> &orig_types, const CTEInfo &left_cte,
	                                            const CTEInfo &right_cte, JoinType join_type);

	idx_t GetCTEColumnIndex(const CTEInfo &cte, ColumnBinding original_binding);

	ClientContext &context;
	Binder &binder;
	ColumnBindingReplacer replacer;

private:
	static constexpr double DEFAULT_BRANCH_SELECTIVITY = 0.20;

	bool IsSimpleTableScan(LogicalOperator &op) const;

	LogicalGet *FindBaseTableScan(LogicalOperator &op) const;

	double EstimateBranchSelectivity(const Branch &branch, const RelationStats &left_stats,
	                                 const RelationStats &right_stats) const;

	idx_t EstimateORJoinOutput(const RelationStats &left_stats, const RelationStats &right_stats,
	                           const vector<Branch> &branches, idx_t left_card, idx_t right_card) const;

	optional_ptr<const BoundColumnRefExpression> ExtractBaseColumnRef(const Expression &expr) const;
};

} // namespace duckdb
