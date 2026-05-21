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

	//! checks if join has OR of equalities we can rewrite
	bool ShouldRewrite(const LogicalAnyJoin &join, const unordered_set<TableIndex> &left_tables,
	                   const unordered_set<TableIndex> &right_tables, vector<Branch> &out_branches) const;

	//! flattens OR tree into list of equality branches
	bool FlattenOR(const Expression &expr, const unordered_set<TableIndex> &left_tables,
	               const unordered_set<TableIndex> &right_tables, vector<Branch> &out) const;

	TableIndex NewTableIndex();

	//! injects row_number window for rowid tracking
	RowIDResult InjectRowID(unique_ptr<LogicalOperator> child, const string &alias);

	unique_ptr<LogicalOperator> MakeCTERef(const CTEInfo &cte, TableIndex ref_idx) const;

	//! builds match cte from all branches (full rewrite path)
	MatchCTEResult BuildMatchCTE(const CTEInfo &left_cte, const CTEInfo &right_cte, ColumnBinding left_rowid,
	                             ColumnBinding right_rowid, const vector<Branch> &branches);

	//! optimized inner join: union of joins without row-IDs
	unique_ptr<LogicalOperator> BuildInnerUnion(unique_ptr<LogicalOperator> left_child,
	                                            unique_ptr<LogicalOperator> right_child,
	                                            vector<ColumnBinding> left_orig_bindings,
	                                            vector<ColumnBinding> right_orig_bindings,
	                                            const vector<ColumnBinding> &orig_bindings,
	                                            const vector<LogicalType> &orig_types, const vector<Branch> &branches);

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

	//! normalizes inner union output to original binding order
	unique_ptr<LogicalOperator> NormaliseInnerUnionOutput(unique_ptr<LogicalOperator> epilogue,
	                                                      const vector<ColumnBinding> &orig_bindings,
	                                                      const vector<LogicalType> &orig_types, idx_t left_col_count,
	                                                      idx_t right_col_count);

	//! normalizes full rewrite output to original binding order
	unique_ptr<LogicalOperator> NormaliseOutput(unique_ptr<LogicalOperator> epilogue,
	                                            const vector<ColumnBinding> &orig_bindings,
	                                            const vector<LogicalType> &orig_types, const CTEInfo &left_cte,
	                                            const CTEInfo &right_cte, JoinType join_type);

	idx_t GetCTEColumnIndex(const CTEInfo &cte, ColumnBinding original_binding);

	ClientContext &context;
	Binder &binder;
	ColumnBindingReplacer replacer;
};

} // namespace duckdb
