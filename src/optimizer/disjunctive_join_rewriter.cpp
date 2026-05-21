#include "duckdb/optimizer/disjunctive_join_rewriter.hpp"

#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

DisjunctiveJoinRewriter::DisjunctiveJoinRewriter(ClientContext &context, Binder &binder)
    : context(context), binder(binder) {
}

TableIndex DisjunctiveJoinRewriter::NewTableIndex() {
	return binder.GenerateTableIndex();
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::Optimize(unique_ptr<LogicalOperator> op) {
	op->ResolveOperatorTypes();
	op = OptimizeInternal(std::move(op));

	return op;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::OptimizeInternal(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child));
	}

	// fix expressions in current operator after children are rewritten
	if (!replacer.replacement_bindings.empty()) {
		replacer.VisitOperatorExpressionsOnly(*op);
	}

	if (op->type != LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return op;
	}
	auto &join = op->Cast<LogicalAnyJoin>();

	unordered_set<TableIndex> left_tables, right_tables;
	for (auto &b : join.children[0]->GetColumnBindings()) {
		left_tables.insert(b.table_index);
	}
	for (auto &b : join.children[1]->GetColumnBindings()) {
		right_tables.insert(b.table_index);
	}

	vector<Branch> branches;
	if (!ShouldRewrite(join, left_tables, right_tables, branches)) {
		return op;
	}

	auto orig_bindings = join.GetColumnBindings();
	auto orig_types = join.types;

	auto left_child = std::move(op->children[0]);
	auto right_child = std::move(op->children[1]);

	vector<ColumnBinding> left_orig_bindings = left_child->GetColumnBindings();
	vector<ColumnBinding> right_orig_bindings = right_child->GetColumnBindings();

	auto left_base = InjectRowID(std::move(left_child), "left_rowid");
	auto right_base = InjectRowID(std::move(right_child), "right_rowid");

	TableIndex left_cte_idx = NewTableIndex();
	TableIndex right_cte_idx = NewTableIndex();

	CTEInfo left_cte {left_cte_idx, left_base.all_types, left_base.all_bindings, std::move(left_orig_bindings)};
	CTEInfo right_cte {right_cte_idx, right_base.all_types, right_base.all_bindings, std::move(right_orig_bindings)};

	auto match_result = BuildMatchCTE(left_cte, right_cte, left_base.rowid_col, right_base.rowid_col, branches);

	TableIndex match_cte_idx = NewTableIndex();
	CTEInfo match_cte {match_cte_idx, match_result.output_types, match_result.output_bindings};

	unique_ptr<LogicalOperator> epilogue;
	switch (join.join_type) {
	case JoinType::INNER:
		epilogue = BuildInner(match_cte, left_cte, right_cte, left_base.rowid_col, right_base.rowid_col);
		break;
	case JoinType::LEFT:
		epilogue = BuildLeft(match_cte, left_cte, right_cte, left_base.rowid_col, right_base.rowid_col);
		break;
	case JoinType::RIGHT:
		epilogue = BuildRight(match_cte, left_cte, right_cte, left_base.rowid_col, right_base.rowid_col);
		break;
	case JoinType::OUTER:
		epilogue = BuildFull(match_cte, left_cte, right_cte, left_base.rowid_col, right_base.rowid_col);
		break;
	case JoinType::SEMI:
		epilogue = BuildSemi(match_cte, left_cte, left_base.rowid_col);
		break;
	case JoinType::ANTI:
		epilogue = BuildAnti(match_cte, left_cte, left_base.rowid_col);
		break;
	default:
		D_ASSERT(false);
		return op;
	}

	epilogue = NormaliseOutput(std::move(epilogue), orig_bindings, orig_types, left_cte, right_cte, join.join_type);

	// wrap in CTEs
	epilogue = make_uniq<LogicalMaterializedCTE>("match_cte", match_cte.table_index, match_cte.output_types.size(),
	                                             std::move(match_result.plan), std::move(epilogue),
	                                             CTEMaterialize::CTE_MATERIALIZE_NEVER);

	epilogue = make_uniq<LogicalMaterializedCTE>("right_cte", right_cte.table_index, right_cte.output_types.size(),
	                                             std::move(right_base.plan), std::move(epilogue),
	                                             CTEMaterialize::CTE_MATERIALIZE_ALWAYS);

	epilogue = make_uniq<LogicalMaterializedCTE>("left_cte", left_cte.table_index, left_cte.output_types.size(),
	                                             std::move(left_base.plan), std::move(epilogue),
	                                             CTEMaterialize::CTE_MATERIALIZE_ALWAYS);

	return epilogue;
}

bool DisjunctiveJoinRewriter::ShouldRewrite(const LogicalAnyJoin &join, const unordered_set<TableIndex> &left_tables,
                                            const unordered_set<TableIndex> &right_tables,
                                            vector<Branch> &out_branches) const {
	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
	case JoinType::SEMI:
	case JoinType::ANTI:
		break;
	default:
		return false;
	}

	if (!join.condition) {
		return false;
	}

	const Expression &expr = *join.condition;
	if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return false;
	}

	if (!FlattenOR(expr, left_tables, right_tables, out_branches)) {
		return false;
	}

	// TODO: derive branch threshold from cardinality estimates
	if (out_branches.size() > 8) {
		return false;
	}

	return out_branches.size() >= 2;
}

bool DisjunctiveJoinRewriter::FlattenOR(const Expression &expr, const unordered_set<TableIndex> &left_tables,
                                        const unordered_set<TableIndex> &right_tables, vector<Branch> &out) const {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (const auto &child : conj.children) {
			if (!FlattenOR(*child, left_tables, right_tables, out)) {
				return false;
			}
		}
		return true;
	}

	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}

	if (expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}

	const auto &comp = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comp);
	auto &right = BoundComparisonExpression::Right(comp);

	auto l_side = JoinSide::GetJoinSide(left, left_tables, right_tables);
	auto r_side = JoinSide::GetJoinSide(right, left_tables, right_tables);

	if (l_side == JoinSide::BOTH || l_side == JoinSide::NONE) {
		return false;
	}

	if (r_side == JoinSide::BOTH || r_side == JoinSide::NONE) {
		return false;
	}

	if (l_side == r_side) {
		return false;
	}

	Branch b;
	if (l_side == JoinSide::LEFT) {
		b.left_expr = left.Copy();
		b.right_expr = right.Copy();
	} else {
		b.left_expr = right.Copy();
		b.right_expr = left.Copy();
	}

	out.push_back(std::move(b));
	return true;
}

DisjunctiveJoinRewriter::RowIDResult DisjunctiveJoinRewriter::InjectRowID(unique_ptr<LogicalOperator> child,
                                                                          const string &alias) {
	TableIndex win_tbl = NewTableIndex();

	auto win_expr = RowNumberFun::GetFunction().Bind(context);

	win_expr->SetAlias(alias);
	win_expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	win_expr->end = WindowBoundary::CURRENT_ROW_ROWS;

	auto child_types = child->types;
	auto child_bindings = child->GetColumnBindings();

	auto win_op = make_uniq<LogicalWindow>(win_tbl);
	win_op->expressions.push_back(std::move(win_expr));
	win_op->AddChild(std::move(child));

	RowIDResult result;
	result.plan = std::move(win_op);
	result.rowid_col = ColumnBinding(win_tbl, ProjectionIndex(0));
	result.all_types = child_types;
	result.all_types.push_back(LogicalType::BIGINT);
	result.all_bindings = child_bindings;
	result.all_bindings.push_back(result.rowid_col);

	return result;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::MakeCTERef(const CTEInfo &cte, TableIndex ref_idx) const {
	vector<string> bound_columns;
	bound_columns.reserve(cte.output_types.size());
	for (idx_t i = 0; i < cte.output_types.size(); ++i) {
		bound_columns.push_back("col_" + to_string(i));
	}

	return make_uniq<LogicalCTERef>(TableIndex(ref_idx), TableIndex(cte.table_index), cte.output_types,
	                                std::move(bound_columns));
}

DisjunctiveJoinRewriter::MatchCTEResult
DisjunctiveJoinRewriter::BuildMatchCTE(const CTEInfo &left_cte, const CTEInfo &right_cte, ColumnBinding left_rowid,
                                       ColumnBinding right_rowid, const vector<Branch> &branches) {
	idx_t left_rid_idx = GetCTEColumnIndex(left_cte, left_rowid);
	idx_t right_rid_idx = GetCTEColumnIndex(right_cte, right_rowid);

	vector<unique_ptr<LogicalOperator>> union_children;
	TableIndex union_tbl = NewTableIndex();

	for (const auto &branch : branches) {
		TableIndex left_ref_idx = NewTableIndex();
		TableIndex right_ref_idx = NewTableIndex();

		auto left_scan = MakeCTERef(left_cte, left_ref_idx);
		auto right_scan = MakeCTERef(right_cte, right_ref_idx);

		auto left_expr = branch.left_expr->Copy();
		auto right_expr = branch.right_expr->Copy();

		// remap left expression to cte-ref bindings
		ColumnBindingReplacer expr_replacer;
		for (idx_t i = 0; i < left_cte.original_bindings.size(); i++) {
			expr_replacer.replacement_bindings.emplace_back(left_cte.original_bindings[i],
			                                                ColumnBinding(left_ref_idx, ProjectionIndex(i)),
			                                                left_cte.output_types[i]);
		}
		expr_replacer.VisitExpression(&left_expr);

		// remap right expression to cte-ref bindings
		expr_replacer.replacement_bindings.clear();
		for (idx_t i = 0; i < right_cte.original_bindings.size(); i++) {
			expr_replacer.replacement_bindings.emplace_back(right_cte.original_bindings[i],
			                                                ColumnBinding(right_ref_idx, ProjectionIndex(i)),
			                                                right_cte.output_types[i]);
		}
		expr_replacer.VisitExpression(&right_expr);

		// filter out nulls so is not null does not match
		auto left_is_not_null =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		left_is_not_null->children.push_back(left_expr->Copy());

		auto right_is_not_null =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		right_is_not_null->children.push_back(right_expr->Copy());

		auto left_filter = make_uniq<LogicalFilter>();
		left_filter->expressions.push_back(std::move(left_is_not_null));
		left_filter->AddChild(std::move(left_scan));

		auto right_filter = make_uniq<LogicalFilter>();
		right_filter->expressions.push_back(std::move(right_is_not_null));
		right_filter->AddChild(std::move(right_scan));

		auto inner_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		inner_join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));
		inner_join->AddChild(std::move(left_filter));
		inner_join->AddChild(std::move(right_filter));

		TableIndex proj_tbl = NewTableIndex();
		vector<unique_ptr<Expression>> proj_exprs;
		proj_exprs.push_back(
		    ColRef(ColumnBinding(left_ref_idx, ProjectionIndex(left_rid_idx)), LogicalType::BIGINT, "match_left"));
		proj_exprs.push_back(
		    ColRef(ColumnBinding(right_ref_idx, ProjectionIndex(right_rid_idx)), LogicalType::BIGINT, "match_right"));

		auto proj = make_uniq<LogicalProjection>(proj_tbl, std::move(proj_exprs));
		proj->AddChild(std::move(inner_join));

		union_children.push_back(std::move(proj));
	}

	bool setop_all = false;
	auto native_union = make_uniq<LogicalSetOperation>(union_tbl, 2, std::move(union_children),
	                                                   LogicalOperatorType::LOGICAL_UNION, setop_all, true);

	MatchCTEResult result;
	result.plan = std::move(native_union);
	result.output_types = {LogicalType::BIGINT, LogicalType::BIGINT};
	result.output_bindings = {ColumnBinding(union_tbl, ProjectionIndex(0)),
	                          ColumnBinding(union_tbl, ProjectionIndex(1))};
	return result;
}

unique_ptr<LogicalOperator>
DisjunctiveJoinRewriter::BuildTwoSidedJoin(const CTEInfo &match_cte, const CTEInfo &left_cte, const CTEInfo &right_cte,
                                           ColumnBinding left_rowid, ColumnBinding right_rowid,
                                           JoinType first_join_type, JoinType second_join_type, bool swap_build_order) {
	TableIndex match_ref = NewTableIndex();
	TableIndex left_ref = NewTableIndex();
	TableIndex right_ref = NewTableIndex();

	idx_t left_rid_idx = GetCTEColumnIndex(left_cte, left_rowid);
	idx_t right_rid_idx = GetCTEColumnIndex(right_cte, right_rowid);

	auto match_scan = MakeCTERef(match_cte, match_ref);
	auto left_scan = MakeCTERef(left_cte, left_ref);
	auto right_scan = MakeCTERef(right_cte, right_ref);

	auto match_bindings = match_scan->GetColumnBindings();
	auto left_bindings = left_scan->GetColumnBindings();
	auto right_bindings = right_scan->GetColumnBindings();
	auto match_types = match_cte.output_types;
	auto left_types = left_cte.output_types;
	auto right_types = right_cte.output_types;

	unique_ptr<LogicalOperator> first_scan, second_scan;
	ColumnBinding first_rowid_bind, second_rowid_bind;
	idx_t first_match_idx, second_match_idx;

	if (!swap_build_order) {
		first_scan = std::move(left_scan);
		second_scan = std::move(right_scan);
		first_rowid_bind = left_bindings[left_rid_idx];
		second_rowid_bind = right_bindings[right_rid_idx];
		first_match_idx = 0;
		second_match_idx = 1;
	} else {
		first_scan = std::move(right_scan);
		second_scan = std::move(left_scan);
		first_rowid_bind = right_bindings[right_rid_idx];
		second_rowid_bind = left_bindings[left_rid_idx];
		first_match_idx = 1;
		second_match_idx = 0;
	}

	auto first_join = make_uniq<LogicalComparisonJoin>(first_join_type);
	first_join->conditions.push_back(JoinCondition(
	    ColRef(first_rowid_bind, LogicalType::BIGINT, "first_rowid"),
	    ColRef(match_bindings[first_match_idx], LogicalType::BIGINT, "match_first"), ExpressionType::COMPARE_EQUAL));
	first_join->AddChild(std::move(first_scan));
	first_join->AddChild(std::move(match_scan));

	auto final_join = make_uniq<LogicalComparisonJoin>(second_join_type);
	final_join->conditions.push_back(
	    JoinCondition(ColRef(match_bindings[second_match_idx], LogicalType::BIGINT, "match_second"),
	                  ColRef(second_rowid_bind, LogicalType::BIGINT, "second_rowid"), ExpressionType::COMPARE_EQUAL));
	final_join->AddChild(std::move(first_join));
	final_join->AddChild(std::move(second_scan));

	return std::move(final_join);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildOneSidedJoin(const CTEInfo &match_cte,
                                                                       const CTEInfo &left_cte,
                                                                       ColumnBinding left_rowid, JoinType join_type) {
	TableIndex match_ref = NewTableIndex();
	TableIndex left_ref = NewTableIndex();
	idx_t left_rid_idx = GetCTEColumnIndex(left_cte, left_rowid);

	auto match_scan = MakeCTERef(match_cte, match_ref);
	auto left_scan = MakeCTERef(left_cte, left_ref);

	auto match_left_bind = ColumnBinding(match_ref, ProjectionIndex(0));
	auto left_rowid_bind = ColumnBinding(left_ref, ProjectionIndex(left_rid_idx));

	auto single_join = make_uniq<LogicalComparisonJoin>(join_type);
	single_join->conditions.push_back(JoinCondition(ColRef(left_rowid_bind, LogicalType::BIGINT, "left_rowid"),
	                                                ColRef(match_left_bind, match_cte.output_types[0], "match_left"),
	                                                ExpressionType::COMPARE_EQUAL));
	single_join->AddChild(std::move(left_scan));
	single_join->AddChild(std::move(match_scan));

	return std::move(single_join);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildInner(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                                const CTEInfo &right_cte, ColumnBinding left_rowid,
                                                                ColumnBinding right_rowid) {
	return BuildTwoSidedJoin(match_cte, left_cte, right_cte, left_rowid, right_rowid, JoinType::INNER, JoinType::INNER);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildLeft(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                               const CTEInfo &right_cte, ColumnBinding left_rowid,
                                                               ColumnBinding right_rowid) {
	return BuildTwoSidedJoin(match_cte, left_cte, right_cte, left_rowid, right_rowid, JoinType::LEFT, JoinType::LEFT);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildRight(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                                const CTEInfo &right_cte, ColumnBinding left_rowid,
                                                                ColumnBinding right_rowid) {
	return BuildTwoSidedJoin(match_cte, left_cte, right_cte, left_rowid, right_rowid, JoinType::LEFT, JoinType::LEFT,
	                         true);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildFull(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                               const CTEInfo &right_cte, ColumnBinding left_rowid,
                                                               ColumnBinding right_rowid) {
	return BuildTwoSidedJoin(match_cte, left_cte, right_cte, left_rowid, right_rowid, JoinType::LEFT, JoinType::OUTER);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildSemi(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                               ColumnBinding left_rowid) {
	return BuildOneSidedJoin(match_cte, left_cte, left_rowid, JoinType::SEMI);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildAnti(const CTEInfo &match_cte, const CTEInfo &left_cte,
                                                               ColumnBinding left_rowid) {
	return BuildOneSidedJoin(match_cte, left_cte, left_rowid, JoinType::ANTI);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::NormaliseOutput(unique_ptr<LogicalOperator> epilogue,
                                                                     const vector<ColumnBinding> &orig_bindings,
                                                                     const vector<LogicalType> &orig_types,
                                                                     const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                     JoinType join_type) {
	auto epilogue_bindings = epilogue->GetColumnBindings();
	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.reserve(orig_bindings.size());

	idx_t left_count = left_cte.output_types.size();
	idx_t right_count = right_cte.output_types.size();
	idx_t orig_left_count = left_count - 1;
	idx_t orig_right_count = right_count - 1;

	bool two_sided = (join_type == JoinType::INNER || join_type == JoinType::LEFT || join_type == JoinType::RIGHT ||
	                  join_type == JoinType::OUTER);
	bool swapped = (join_type == JoinType::RIGHT);

	if (two_sided) {
		if (!swapped) {
			for (idx_t i = 0; i < orig_left_count; i++) {
				proj_exprs.push_back(ColRef(epilogue_bindings[i], orig_types[i]));
			}
			for (idx_t i = 0; i < orig_right_count; i++) {
				proj_exprs.push_back(ColRef(epilogue_bindings[left_count + 2 + i], orig_types[orig_left_count + i]));
			}
		} else {
			for (idx_t i = 0; i < orig_left_count; i++) {
				proj_exprs.push_back(ColRef(epilogue_bindings[right_count + 2 + i], orig_types[i]));
			}
			for (idx_t i = 0; i < orig_right_count; i++) {
				proj_exprs.push_back(ColRef(epilogue_bindings[i], orig_types[orig_left_count + i]));
			}
		}
	} else {
		for (idx_t i = 0; i < orig_left_count; i++) {
			proj_exprs.push_back(ColRef(epilogue_bindings[i], orig_types[i]));
		}
	}

	D_ASSERT(proj_exprs.size() == orig_bindings.size());

	TableIndex norm_tbl = NewTableIndex();
	auto proj = make_uniq<LogicalProjection>(norm_tbl, std::move(proj_exprs));
	proj->AddChild(std::move(epilogue));

	for (idx_t i = 0; i < orig_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(orig_bindings[i], ColumnBinding(norm_tbl, ProjectionIndex(i)),
		                                           orig_types[i]);
	}

	return std::move(proj);
}

unique_ptr<Expression> DisjunctiveJoinRewriter::ColRef(ColumnBinding binding, const LogicalType &type,
                                                       const string &alias) {
	return make_uniq<BoundColumnRefExpression>(alias, type, binding);
}

idx_t DisjunctiveJoinRewriter::GetCTEColumnIndex(const CTEInfo &cte, ColumnBinding original_binding) {
	for (idx_t i = 0; i < cte.output_bindings.size(); i++) {
		if (cte.output_bindings[i] == original_binding) {
			return i;
		}
	}
	throw InternalException("Binding not found in CTE");
}

} // namespace duckdb
