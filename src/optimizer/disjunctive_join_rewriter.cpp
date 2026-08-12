#include "duckdb/optimizer/disjunctive_join_rewriter.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

DisjunctiveJoinRewriter::DisjunctiveJoinRewriter(ClientContext &context, Binder &binder)
    : context(context), binder(binder) {
}

TableIndex DisjunctiveJoinRewriter::NewTableIndex() {
	return binder.GenerateTableIndex();
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::Optimize(unique_ptr<LogicalOperator> op) {
	op->ResolveOperatorTypes();
	return OptimizeInternal(std::move(op));
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::OptimizeInternal(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child));
	}

	// Fix expressions in current operator after children are rewritten
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

	TableIndex left_cte_idx = NewTableIndex();
	TableIndex right_cte_idx = NewTableIndex();

	CTEInfo left_cte {left_cte_idx, left_child->types, left_child->GetColumnBindings(),
	                  left_child->GetColumnBindings()};
	CTEInfo right_cte {right_cte_idx, right_child->types, right_child->GetColumnBindings(),
	                   right_child->GetColumnBindings()};

	unique_ptr<LogicalOperator> epilogue;
	switch (join.join_type) {
	case JoinType::INNER:
		epilogue = BuildInnerJoin(left_cte, right_cte, branches);
		break;
	case JoinType::LEFT:
		epilogue = BuildLeftJoin(left_cte, right_cte, branches);
		break;
	case JoinType::RIGHT:
		epilogue = BuildRightJoin(left_cte, right_cte, branches);
		break;
	case JoinType::OUTER:
		epilogue = BuildFullJoin(left_cte, right_cte, branches);
		break;
	case JoinType::SEMI:
		epilogue = BuildSemiJoin(left_cte, right_cte, branches);
		break;
	case JoinType::ANTI:
		epilogue = BuildAntiJoin(left_cte, right_cte, branches);
		break;
	default:
		return op;
	}

	epilogue = NormalizeOutput(std::move(epilogue), orig_bindings, orig_types, left_cte, right_cte, join.join_type);

	// Wrap in CTEs
	epilogue = make_uniq<LogicalMaterializedCTE>("rhs_cte", right_cte.table_index, right_cte.output_types.size(),
	                                             std::move(right_child), std::move(epilogue),
	                                             CTEMaterialize::CTE_MATERIALIZE_DEFAULT);

	epilogue = make_uniq<LogicalMaterializedCTE>("lhs_cte", left_cte.table_index, left_cte.output_types.size(),
	                                             std::move(left_child), std::move(epilogue),
	                                             CTEMaterialize::CTE_MATERIALIZE_DEFAULT);

	return epilogue;
}

bool DisjunctiveJoinRewriter::ShouldRewrite(const LogicalOperator &join, const unordered_set<TableIndex> &left_tables,
                                            const unordered_set<TableIndex> &right_tables,
                                            vector<Branch> &out_branches) const {
	auto &any_join = join.Cast<LogicalAnyJoin>();
	switch (any_join.join_type) {
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

	if (!any_join.condition) {
		return false;
	}

	const Expression &expr = *any_join.condition;
	if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return false;
	}

	if (!FlattenOR(expr, left_tables, right_tables, out_branches)) {
		return false;
	}

	return out_branches.size() >= 2;
}

bool DisjunctiveJoinRewriter::FlattenOR(const Expression &expr, const unordered_set<TableIndex> &left_tables,
                                        const unordered_set<TableIndex> &right_tables, vector<Branch> &out) const {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (const auto &child : conj.GetChildren()) {
			if (!FlattenOR(*child, left_tables, right_tables, out)) {
				return false;
			}
		}
		return true;
	}

	if (!BoundComparisonExpression::IsComparison(expr) || expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}

	const auto &comp = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comp);
	auto &right = BoundComparisonExpression::Right(comp);

	auto l_side = JoinSide::GetJoinSide(left, left_tables, right_tables);
	auto r_side = JoinSide::GetJoinSide(right, left_tables, right_tables);

	if (l_side == JoinSide::BOTH || l_side == JoinSide::NONE || r_side == JoinSide::BOTH || r_side == JoinSide::NONE ||
	    l_side == r_side) {
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

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildInnerJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                    const vector<Branch> &branches) {
	auto children = CreateMatchedBranches(left_cte, right_cte, branches);
	return CreateUnionAll(std::move(children), left_cte.output_types.size() + right_cte.output_types.size());
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildLeftJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                   const vector<Branch> &branches) {
	auto children = CreateMatchedBranches(left_cte, right_cte, branches);
	children.push_back(CreateUnmatchedProbeSide(left_cte, right_cte, branches));
	return CreateUnionAll(std::move(children), left_cte.output_types.size() + right_cte.output_types.size());
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildRightJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                    const vector<Branch> &branches) {
	CTEInfo swapped_left {right_cte.table_index, right_cte.output_types, right_cte.output_bindings,
	                      right_cte.original_bindings};
	CTEInfo swapped_right {left_cte.table_index, left_cte.output_types, left_cte.output_bindings,
	                       left_cte.original_bindings};
	return BuildLeftJoin(swapped_left, swapped_right, SwapBranches(branches));
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildFullJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                   const vector<Branch> &branches) {
	auto children = CreateMatchedBranches(left_cte, right_cte, branches);
	children.push_back(CreateUnmatchedProbeSide(left_cte, right_cte, branches));
	children.push_back(CreateUnmatchedBuildSide(left_cte, right_cte, branches));
	return CreateUnionAll(std::move(children), left_cte.output_types.size() + right_cte.output_types.size());
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildSemiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                   const vector<Branch> &branches) {
	TableIndex probe_ref = NewTableIndex();
	auto current = CreateCTERef(left_cte, probe_ref);
	vector<unique_ptr<Expression>> marker_refs;

	for (idx_t i = 0; i < branches.size(); i++) {
		TableIndex build_ref = NewTableIndex();
		TableIndex mark_tbl = NewTableIndex();

		auto build_scan = CreateCTERef(right_cte, build_ref);

		auto left_expr = branches[i].left_expr->Copy();
		auto right_expr = branches[i].right_expr->Copy();
		RemapExpressions(left_cte, right_cte, probe_ref, build_ref, left_expr, right_expr);

		auto mark_join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
		mark_join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));
		mark_join->mark_index = mark_tbl;
		mark_join->AddChild(std::move(current));
		mark_join->AddChild(std::move(build_scan));

		current = std::move(mark_join);
		marker_refs.push_back(
		    CreateColRef(ColumnBinding(mark_tbl, ProjectionIndex(0)), LogicalType::BOOLEAN, "mark_" + to_string(i)));
	}

	unique_ptr<Expression> filter_cond;
	if (marker_refs.size() == 1) {
		filter_cond = std::move(marker_refs[0]);
	} else {
		filter_cond = std::move(marker_refs[0]);
		for (idx_t i = 1; i < marker_refs.size(); i++) {
			auto or_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
			or_expr->GetChildrenMutable().push_back(std::move(filter_cond));
			or_expr->GetChildrenMutable().push_back(std::move(marker_refs[i]));
			filter_cond = std::move(or_expr);
		}
	}

	auto filter = make_uniq<LogicalFilter>();
	filter->expressions.push_back(std::move(filter_cond));
	filter->AddChild(std::move(current));

	return filter;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::BuildAntiJoin(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                   const vector<Branch> &branches) {
	TableIndex probe_ref = NewTableIndex();
	auto chain = CreateAntiJoinChain(left_cte, right_cte, branches, probe_ref);
	return chain;
}

vector<unique_ptr<LogicalOperator>> DisjunctiveJoinRewriter::CreateMatchedBranches(const CTEInfo &left_cte,
                                                                                   const CTEInfo &right_cte,
                                                                                   const vector<Branch> &branches) {
	vector<unique_ptr<LogicalOperator>> union_children;

	for (idx_t i = 0; i < branches.size(); i++) {
		TableIndex left_ref_idx = NewTableIndex();
		TableIndex right_ref_idx = NewTableIndex();

		vector<unique_ptr<Expression>> exclusion_preds;
		if (i > 0) {
			exclusion_preds = CreateExclusionPredicates(left_cte, right_cte, branches, i, left_ref_idx, right_ref_idx);
		}

		auto left_scan = CreateCTERef(left_cte, left_ref_idx);
		auto right_scan = CreateCTERef(right_cte, right_ref_idx);

		auto left_expr = branches[i].left_expr->Copy();
		auto right_expr = branches[i].right_expr->Copy();
		RemapExpressions(left_cte, right_cte, left_ref_idx, right_ref_idx, left_expr, right_expr);

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));

		if (!exclusion_preds.empty()) {
			for (const auto &pred : exclusion_preds) {
				join->conditions.push_back(JoinCondition(pred->Copy()));
			}
		}

		join->AddChild(std::move(left_scan));
		join->AddChild(std::move(right_scan));

		union_children.push_back(std::move(join));
	}
	return union_children;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateUnmatchedProbeSide(const CTEInfo &probe_cte,
                                                                              const CTEInfo &build_cte,
                                                                              const vector<Branch> &branches) {
	TableIndex probe_ref = NewTableIndex();
	auto anti_chain = CreateAntiJoinChain(probe_cte, build_cte, branches, probe_ref);

	vector<unique_ptr<Expression>> proj_exprs;
	for (idx_t col = 0; col < probe_cte.output_types.size(); col++) {
		proj_exprs.push_back(CreateColRef(ColumnBinding(probe_ref, ProjectionIndex(col)), probe_cte.output_types[col]));
	}
	for (idx_t col = 0; col < build_cte.output_types.size(); col++) {
		proj_exprs.push_back(CreateNullExpr(build_cte.output_types[col]));
	}
	return CreateProjection(std::move(anti_chain), std::move(proj_exprs));
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateUnmatchedBuildSide(const CTEInfo &left_cte,
                                                                              const CTEInfo &right_cte,
                                                                              const vector<Branch> &branches) {
	vector<unique_ptr<LogicalOperator>> matched_children;
	for (idx_t i = 0; i < branches.size(); i++) {
		TableIndex left_ref_idx = NewTableIndex();
		TableIndex right_ref_idx = NewTableIndex();

		vector<unique_ptr<Expression>> exclusion_preds;
		if (i > 0) {
			exclusion_preds = CreateExclusionPredicates(left_cte, right_cte, branches, i, left_ref_idx, right_ref_idx);
		}

		auto left_scan = CreateCTERef(left_cte, left_ref_idx);
		auto right_scan = CreateCTERef(right_cte, right_ref_idx);

		auto left_expr = branches[i].left_expr->Copy();
		auto right_expr = branches[i].right_expr->Copy();
		RemapExpressions(left_cte, right_cte, left_ref_idx, right_ref_idx, left_expr, right_expr);

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));

		if (!exclusion_preds.empty()) {
			for (const auto &pred : exclusion_preds) {
				join->conditions.push_back(JoinCondition(pred->Copy()));
			}
		}

		join->AddChild(std::move(left_scan));
		join->AddChild(std::move(right_scan));

		auto proj_exprs = CreateColRefs(right_ref_idx, right_cte.output_types);
		matched_children.push_back(CreateProjection(std::move(join), std::move(proj_exprs)));
	}

	auto matched_rows_plan = CreateUnionAll(std::move(matched_children), right_cte.output_types.size());
	auto matched_union_tbl = matched_rows_plan->GetColumnBindings()[0].table_index;

	TableIndex rhs_ref = NewTableIndex();
	auto rhs_scan = CreateCTERef(right_cte, rhs_ref);

	auto anti_join = make_uniq<LogicalComparisonJoin>(JoinType::ANTI);

	for (idx_t col = 0; col < right_cte.output_types.size(); col++) {
		auto left_expr = CreateColRef(ColumnBinding(rhs_ref, ProjectionIndex(col)), right_cte.output_types[col]);
		auto right_expr =
		    CreateColRef(ColumnBinding(matched_union_tbl, ProjectionIndex(col)), right_cte.output_types[col]);
		anti_join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));
	}

	anti_join->AddChild(std::move(rhs_scan));
	anti_join->AddChild(std::move(matched_rows_plan));

	// [NULL(build_cols)..., probe_cols...]
	auto null_exprs = CreateNullExprs(left_cte.output_types);
	auto right_exprs = CreateColRefs(rhs_ref, right_cte.output_types);

	null_exprs.insert(null_exprs.end(), std::make_move_iterator(right_exprs.begin()),
	                  std::make_move_iterator(right_exprs.end()));

	return CreateProjection(std::move(anti_join), std::move(null_exprs));
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateAntiJoinChain(const CTEInfo &probe_cte,
                                                                         const CTEInfo &build_cte,
                                                                         const vector<Branch> &branches,
                                                                         TableIndex probe_ref_idx) {
	auto current = CreateCTERef(probe_cte, probe_ref_idx);

	for (idx_t i = 0; i < branches.size(); i++) {
		TableIndex build_ref = NewTableIndex();
		auto build_scan = CreateCTERef(build_cte, build_ref);

		auto left_expr = branches[i].left_expr->Copy();
		auto right_expr = branches[i].right_expr->Copy();
		RemapExpressions(probe_cte, build_cte, probe_ref_idx, build_ref, left_expr, right_expr);

		auto anti_join = make_uniq<LogicalComparisonJoin>(JoinType::ANTI);
		anti_join->conditions.push_back(
		    JoinCondition(std::move(left_expr), std::move(right_expr), ExpressionType::COMPARE_EQUAL));
		anti_join->AddChild(std::move(current));
		anti_join->AddChild(std::move(build_scan));

		current = std::move(anti_join);
	}
	return current;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateUnionAll(vector<unique_ptr<LogicalOperator>> children,
                                                                    idx_t total_columns) {
	TableIndex union_tbl = NewTableIndex();
	return make_uniq<LogicalSetOperation>(union_tbl, total_columns, std::move(children),
	                                      LogicalOperatorType::LOGICAL_UNION, true, true);
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::NormalizeOutput(unique_ptr<LogicalOperator> plan,
                                                                     const vector<ColumnBinding> &orig_bindings,
                                                                     const vector<LogicalType> &orig_types,
                                                                     const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                                     JoinType join_type) {
	auto current_bindings = plan->GetColumnBindings();
	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.reserve(orig_bindings.size());

	bool two_sided = (join_type == JoinType::INNER || join_type == JoinType::LEFT || join_type == JoinType::RIGHT ||
	                  join_type == JoinType::OUTER);
	bool swapped = (join_type == JoinType::RIGHT);

	idx_t left_col_count = left_cte.output_types.size();
	idx_t right_col_count = right_cte.output_types.size();

	if (two_sided) {
		if (!swapped) {
			// Normal order: [left_cols..., right_cols...]
			// Current plan should already be in this order from our builders
			for (idx_t i = 0; i < orig_bindings.size(); i++) {
				proj_exprs.push_back(CreateColRef(current_bindings[i], orig_types[i]));
			}
		} else {
			// Swapped order (RIGHT join): our builder produces [right_cols..., left_cols...]
			// Need to map back to [left_cols..., right_cols...]
			for (idx_t i = 0; i < left_col_count; i++) {
				proj_exprs.push_back(CreateColRef(current_bindings[right_col_count + i], orig_types[i]));
			}
			for (idx_t i = 0; i < right_col_count; i++) {
				proj_exprs.push_back(CreateColRef(current_bindings[i], orig_types[left_col_count + i]));
			}
		}
	} else {
		// One-sided (SEMI/ANTI): only left columns
		for (idx_t i = 0; i < orig_bindings.size(); i++) {
			proj_exprs.push_back(CreateColRef(current_bindings[i], orig_types[i]));
		}
	}

	D_ASSERT(proj_exprs.size() == orig_bindings.size());

	TableIndex norm_tbl = NewTableIndex();
	auto proj = make_uniq<LogicalProjection>(norm_tbl, std::move(proj_exprs));
	proj->AddChild(std::move(plan));

	// Update replacer so parent operators know about the new bindings
	for (idx_t i = 0; i < orig_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(orig_bindings[i], ColumnBinding(norm_tbl, ProjectionIndex(i)),
		                                           orig_types[i]);
	}
	return proj;
}

void DisjunctiveJoinRewriter::RemapExpressions(const CTEInfo &left_cte, const CTEInfo &right_cte, TableIndex left_ref,
                                               TableIndex right_ref, unique_ptr<Expression> &left_expr,
                                               unique_ptr<Expression> &right_expr) {
	ColumnBindingReplacer expr_replacer;
	for (idx_t col = 0; col < left_cte.original_bindings.size(); col++) {
		expr_replacer.replacement_bindings.emplace_back(
		    left_cte.original_bindings[col], ColumnBinding(left_ref, ProjectionIndex(col)), left_cte.output_types[col]);
	}
	expr_replacer.VisitExpression(&left_expr);

	expr_replacer.replacement_bindings.clear();
	for (idx_t col = 0; col < right_cte.original_bindings.size(); col++) {
		expr_replacer.replacement_bindings.emplace_back(right_cte.original_bindings[col],
		                                                ColumnBinding(right_ref, ProjectionIndex(col)),
		                                                right_cte.output_types[col]);
	}
	expr_replacer.VisitExpression(&right_expr);
}

vector<unique_ptr<Expression>>
DisjunctiveJoinRewriter::CreateExclusionPredicates(const CTEInfo &left_cte, const CTEInfo &right_cte,
                                                   const vector<Branch> &branches, idx_t current_branch_idx,
                                                   TableIndex left_ref_idx, TableIndex right_ref_idx) {
	vector<unique_ptr<Expression>> exclusions;

	for (idx_t i = 0; i < current_branch_idx; i++) {
		auto earlier_left = branches[i].left_expr->Copy();
		auto earlier_right = branches[i].right_expr->Copy();
		RemapExpressions(left_cte, right_cte, left_ref_idx, right_ref_idx, earlier_left, earlier_right);

		auto left_is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		left_is_null->GetChildrenMutable().push_back(earlier_left->Copy());

		auto right_is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		right_is_null->GetChildrenMutable().push_back(earlier_right->Copy());

		auto not_equal = BoundComparisonExpression::Create(ExpressionType::COMPARE_NOTEQUAL, std::move(earlier_left),
		                                                   std::move(earlier_right));

		auto or_nulls = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		or_nulls->GetChildrenMutable().push_back(std::move(left_is_null));
		or_nulls->GetChildrenMutable().push_back(std::move(right_is_null));

		auto final_or = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		final_or->GetChildrenMutable().push_back(std::move(or_nulls));
		final_or->GetChildrenMutable().push_back(std::move(not_equal));

		exclusions.push_back(std::move(final_or));
	}
	return exclusions;
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateCTERef(const CTEInfo &cte, TableIndex ref_idx) {
	vector<Identifier> bound_columns;
	bound_columns.reserve(cte.output_types.size());
	for (idx_t i = 0; i < cte.output_types.size(); ++i) {
		bound_columns.emplace_back("col_" + to_string(i));
	}
	return make_uniq<LogicalCTERef>(TableIndex(ref_idx), TableIndex(cte.table_index), cte.output_types,
	                                std::move(bound_columns));
}

unique_ptr<LogicalOperator> DisjunctiveJoinRewriter::CreateProjection(unique_ptr<LogicalOperator> child,
                                                                      vector<unique_ptr<Expression>> expressions) {
	TableIndex proj_tbl = NewTableIndex();
	auto proj = make_uniq<LogicalProjection>(proj_tbl, std::move(expressions));
	proj->AddChild(std::move(child));
	return proj;
}

unique_ptr<Expression> DisjunctiveJoinRewriter::CreateColRef(ColumnBinding binding, const LogicalType &type,
                                                             const string &alias) {
	return make_uniq<BoundColumnRefExpression>(Identifier(alias), type, binding);
}

unique_ptr<Expression> DisjunctiveJoinRewriter::CreateNullExpr(const LogicalType &type) {
	return make_uniq<BoundConstantExpression>(Value(type));
}

vector<unique_ptr<Expression>> DisjunctiveJoinRewriter::CreateColRefs(TableIndex table_index,
                                                                      const vector<LogicalType> &types) {
	vector<unique_ptr<Expression>> exprs;
	exprs.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		exprs.push_back(CreateColRef(ColumnBinding(table_index, ProjectionIndex(i)), types[i]));
	}
	return exprs;
}

vector<unique_ptr<Expression>> DisjunctiveJoinRewriter::CreateNullExprs(const vector<LogicalType> &types) {
	vector<unique_ptr<Expression>> exprs;
	exprs.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		exprs.push_back(make_uniq<BoundConstantExpression>(Value(types[i])));
	}
	return exprs;
}

vector<DisjunctiveJoinRewriter::Branch> DisjunctiveJoinRewriter::SwapBranches(const vector<Branch> &branches) {
	vector<Branch> swapped;
	for (auto &branch : branches) {
		Branch b;
		b.left_expr = branch.right_expr->Copy();
		b.right_expr = branch.left_expr->Copy();
		swapped.push_back(std::move(b));
	}
	return swapped;
}

} // namespace duckdb
