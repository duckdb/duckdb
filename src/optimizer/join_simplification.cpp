#include "duckdb/optimizer/join_simplification.hpp"

#include "duckdb/optimizer/join_to_semi_join.hpp"
#include "duckdb/optimizer/self_join_aggregate.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Setup
//===--------------------------------------------------------------------===//

JoinSimplification::JoinSimplification(ClientContext &context, JoinSimplificationPhase phase_p) : phase(phase_p) {
	if (phase == JoinSimplificationPhase::BEFORE_FILTER_PUSHDOWN) {
		owned_join_to_semi_join = make_uniq<JoinToSemiJoin>(context);
		join_to_semi_join = owned_join_to_semi_join.get();
	}
}

JoinSimplification::JoinSimplification(JoinSimplification &parent_context, column_binding_set_t required_columns_p)
    : phase(parent_context.phase), join_to_semi_join(parent_context.join_to_semi_join),
      required_columns(std::move(required_columns_p)), initialized_required_columns(true) {
}

JoinSimplification::~JoinSimplification() {
}

void JoinSimplification::Optimize(unique_ptr<LogicalOperator> &plan) {
	if (join_to_semi_join) {
		join_to_semi_join->Initialize(*plan);
	}
	VisitOperator(plan);
}

//===--------------------------------------------------------------------===//
// Parent Info
//===--------------------------------------------------------------------===//

JoinSimplification::ParentInfo JoinSimplification::UnknownInfo() {
	return ParentInfo();
}

JoinSimplification::ParentInfo JoinSimplification::PassThroughInfo(const LogicalOperator &op,
                                                                   bool ignores_duplicates) const {
	ParentInfo info;
	info.ignores_duplicates = ignores_duplicates;
	info.reads_known = parent.reads_known;
	if (parent.reads_known) {
		info.read_tables = parent.read_tables;
		JoinToSemiJoin::AddReadTables(op, info.read_tables);
	}
	info.in_region = JoinToSemiJoin::IsRegionNode(op);
	return info;
}

void JoinSimplification::VisitChild(unique_ptr<LogicalOperator> &child, ParentInfo info) {
	auto saved = std::move(parent);
	parent = std::move(info);
	VisitOperator(child);
	parent = std::move(saved);
}

void JoinSimplification::VisitChildInNewContext(unique_ptr<LogicalOperator> &child,
                                                column_binding_set_t child_required_columns, ParentInfo info) {
	JoinSimplification child_simplification(*this, std::move(child_required_columns));
	child_simplification.parent = std::move(info);
	child_simplification.VisitOperator(child);
}

//===--------------------------------------------------------------------===//
// Column Binding Helpers
//===--------------------------------------------------------------------===//

void JoinSimplification::AddColumnReferences(const Expression &expr, column_binding_set_t &bindings) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0) {
			bindings.insert(colref.Binding());
		}
	});
}

void JoinSimplification::AddRequiredColumns(const Expression &expr) {
	AddColumnReferences(expr, required_columns);
}

void JoinSimplification::AddRequiredColumns(const JoinCondition &condition) {
	if (condition.IsComparison()) {
		AddRequiredColumns(condition.GetLHS());
		AddRequiredColumns(condition.GetRHS());
	} else {
		AddRequiredColumns(condition.GetJoinExpression());
	}
}

void JoinSimplification::AddRequiredColumns(const vector<JoinCondition> &conditions) {
	for (const auto &condition : conditions) {
		AddRequiredColumns(condition);
	}
}

bool JoinSimplification::GetColumnBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		if (!BoundCastExpression::IsCast(expr)) {
			return false;
		}
		auto &cast = expr.Cast<BoundFunctionExpression>();
		return GetColumnBinding(BoundCastExpression::Child(cast), binding);
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	binding = colref.Binding();
	return true;
}

bool JoinSimplification::GetNullPreservingColumnBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		if (!BoundCastExpression::IsCast(expr)) {
			return false;
		}
		auto &cast = expr.Cast<BoundFunctionExpression>();
		if (BoundCastExpression::IsTryCast(cast) ||
		    !BoundCastExpression::CastIsInvertible(BoundCastExpression::SourceType(cast), cast.GetReturnType())) {
			return false;
		}
		return GetNullPreservingColumnBinding(BoundCastExpression::Child(cast), binding);
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	binding = colref.Binding();
	return true;
}

//===--------------------------------------------------------------------===//
// NULL Constraint Tracking
//===--------------------------------------------------------------------===//

void JoinSimplification::HandleExpression(const Expression &expr) {
	ColumnBinding binding;
	if (GetColumnBinding(expr, binding)) {
		null_filtered_columns.insert(binding);
	}
}

void JoinSimplification::HandleFilterExpression(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		const auto &is_not_null = expr.Cast<BoundOperatorExpression>();
		HandleExpression(*is_not_null.GetChildren()[0]);
		AddRequiredColumns(expr);
		return;
	}

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
		const auto &is_null = expr.Cast<BoundOperatorExpression>();
		if (!HandleIsNullExpression(*is_null.GetChildren()[0])) {
			AddRequiredColumns(expr);
		}
		return;
	}

	if (!BoundComparisonExpression::IsComparison(expr)) {
		AddRequiredColumns(expr);
		return;
	}

	AddRequiredColumns(expr);
	if (!FiltersNulls(expr.GetExpressionType())) {
		return;
	}
	const auto &comparison = expr.Cast<BoundFunctionExpression>();
	HandleExpression(BoundComparisonExpression::Left(comparison));
	HandleExpression(BoundComparisonExpression::Right(comparison));
}

bool JoinSimplification::HandleIsNullExpression(const Expression &expr) {
	ColumnBinding binding;
	if (GetNullPreservingColumnBinding(expr, binding)) {
		null_required_columns.insert(binding);
		return true;
	}
	return false;
}

bool JoinSimplification::IsNullFilter(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
	    expr.GetExpressionType() != ExpressionType::OPERATOR_IS_NULL) {
		return false;
	}
	auto &is_null = expr.Cast<BoundOperatorExpression>();
	auto &child = *is_null.GetChildren()[0];
	return GetNullPreservingColumnBinding(child, binding);
}

void JoinSimplification::InitializeRequiredColumns(LogicalOperator &op) {
	if (initialized_required_columns) {
		return;
	}
	for (const auto &binding : op.GetColumnBindings()) {
		required_columns.insert(binding);
	}
	initialized_required_columns = true;
}

bool JoinSimplification::FiltersNulls(ExpressionType comparison_type) {
	return comparison_type != ExpressionType::COMPARE_DISTINCT_FROM &&
	       comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

//===--------------------------------------------------------------------===//
// Join Rewrite Helpers
//===--------------------------------------------------------------------===//

vector<ColumnBinding> JoinSimplification::GetRightBindings(LogicalJoin &join) {
	return LogicalOperator::MapBindings(join.children[1]->GetColumnBindings(), join.right_projection_map);
}

bool JoinSimplification::HasNullRequiredColumns(const vector<ColumnBinding> &bindings) {
	for (const auto &binding : bindings) {
		if (null_required_columns.find(binding) != null_required_columns.end()) {
			return true;
		}
	}
	return false;
}

bool JoinSimplification::HasRequiredColumns(const vector<ColumnBinding> &bindings) {
	for (const auto &binding : bindings) {
		if (required_columns.find(binding) != required_columns.end()) {
			return true;
		}
	}
	return false;
}

void JoinSimplification::EraseNullRequiredColumns(LogicalOperator &child) {
	for (const auto &binding : child.GetColumnBindings()) {
		null_required_columns.erase(binding);
	}
}

void JoinSimplification::MarkEliminatedNullColumns(const vector<ColumnBinding> &bindings) {
	for (const auto &binding : bindings) {
		eliminated_null_columns.insert(binding);
	}
}

bool JoinSimplification::TryConvertLeftToAntiJoin(LogicalComparisonJoin &join) {
	if (join.join_type != JoinType::LEFT) {
		return false;
	}

	auto right_bindings = GetRightBindings(join);
	if (!HasNullRequiredColumns(right_bindings) || HasRequiredColumns(right_bindings)) {
		return false;
	}

	column_binding_set_t match_non_null_rhs;
	for (const auto &condition : join.conditions) {
		if (!condition.IsComparison() || !FiltersNulls(condition.GetComparisonType())) {
			continue;
		}
		ColumnBinding rhs_binding;
		if (!GetColumnBinding(condition.GetRHS(), rhs_binding)) {
			continue;
		}
		match_non_null_rhs.insert(rhs_binding);
	}

	for (const auto &binding : right_bindings) {
		if (null_required_columns.find(binding) == null_required_columns.end()) {
			continue;
		}
		if (match_non_null_rhs.find(binding) == match_non_null_rhs.end()) {
			continue;
		}
		join.join_type = JoinType::ANTI;
		return true;
	}
	return false;
}

void JoinSimplification::SimplifyOuterJoinType(LogicalComparisonJoin &join) {
	bool preserves_null_extended_rows[2] = {join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER,
	                                        join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER};
	for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
		for (const auto &binding : join.children[child_idx]->GetColumnBindings()) {
			if (null_filtered_columns.find(binding) != null_filtered_columns.end()) {
				// Rejecting NULLs in one child removes preservation of NULL extended rows for the other child.
				preserves_null_extended_rows[1 - child_idx] = false;
				break;
			}
		}
	}

	if (!preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
		join.join_type = JoinType::INNER;
		return;
	}

	if (preserves_null_extended_rows[0] && !preserves_null_extended_rows[1]) {
		D_ASSERT(join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER);
		join.join_type = JoinType::LEFT;
	} else if (!preserves_null_extended_rows[0] && preserves_null_extended_rows[1]) {
		D_ASSERT(join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER);
		join.join_type = JoinType::RIGHT;
	} else {
		D_ASSERT(join.join_type == JoinType::OUTER);
		join.join_type = JoinType::OUTER;
	}
}

//===--------------------------------------------------------------------===//
// Operator Visitors
//===--------------------------------------------------------------------===//

//! Which children of a join of this type may see repeated rows of their input without a change of the result of a
//! parent that ignores duplicates (`ignores`): the rows INNER and outer joins emit, as a set, depend on their inputs
//! as sets only, and the right side of a SEMI, ANTI or MARK join only says whether a match exists
static void JoinSidesIgnoreDuplicates(JoinType join_type, bool ignores, bool &left, bool &right) {
	left = false;
	right = false;
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		left = right = ignores;
		break;
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		left = ignores;
		right = true;
		break;
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		left = true;
		right = ignores;
		break;
	default:
		break;
	}
}

void JoinSimplification::VisitJoinChildren(LogicalOperator &op, bool left_ignores_duplicates,
                                           bool right_ignores_duplicates, bool keep_context) {
	const bool deterministic = !JoinToSemiJoin::HasVolatileExpression(op);
	auto left_info = PassThroughInfo(op, left_ignores_duplicates && deterministic);
	auto right_info = PassThroughInfo(op, right_ignores_duplicates && deterministic);
	if (keep_context) {
		VisitChild(op.children[0], std::move(left_info));
		VisitChild(op.children[1], std::move(right_info));
		return;
	}
	// the column sets do not pass through this join: each child starts with all of its columns required
	for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
		auto &child = op.children[child_idx];
		column_binding_set_t child_required_columns;
		for (const auto &binding : child->GetColumnBindings()) {
			child_required_columns.insert(binding);
		}
		VisitChildInNewContext(child, std::move(child_required_columns),
		                       child_idx == 0 ? std::move(left_info) : std::move(right_info));
	}
}

void JoinSimplification::VisitComparisonJoin(LogicalComparisonJoin &join, unique_ptr<LogicalOperator> &op_ptr) {
	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::SEMI:
		VisitInnerOrSemiJoin(join, *op_ptr);
		return;
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		VisitOuterJoin(join, op_ptr);
		return;
	default:
		break;
	}
	bool left;
	bool right;
	JoinSidesIgnoreDuplicates(join.join_type, parent.ignores_duplicates, left, right);
	VisitJoinChildren(*op_ptr, left, right, false);
}

void JoinSimplification::VisitInnerOrSemiJoin(LogicalComparisonJoin &join, LogicalOperator &op) {
	for (const auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			continue;
		}
		AddRequiredColumns(condition);
		if (!FiltersNulls(condition.GetComparisonType())) {
			continue;
		}
		HandleExpression(condition.GetLHS());
		HandleExpression(condition.GetRHS());
	}
	bool left;
	bool right;
	JoinSidesIgnoreDuplicates(join.join_type, parent.ignores_duplicates, left, right);
	VisitJoinChildren(op, left, right, true);
}

void JoinSimplification::VisitOuterJoin(LogicalComparisonJoin &join, unique_ptr<LogicalOperator> &op_ptr) {
	auto &op = *op_ptr;
	bool left;
	bool right;
	if (SimplifiesOuterJoins() && TryConvertLeftToAntiJoin(join)) {
		auto right_bindings = GetRightBindings(join);
		AddRequiredColumns(join.conditions);
		EraseNullRequiredColumns(*join.children[1]);
		JoinSidesIgnoreDuplicates(join.join_type, parent.ignores_duplicates, left, right);
		VisitJoinChildren(op, left, right, true);
		// only operators above the join see the right side as NULL, not the right child itself
		MarkEliminatedNullColumns(right_bindings);
		return;
	}

	if (SimplifiesOuterJoins()) {
		SimplifyOuterJoinType(join);
		if (join.join_type == JoinType::INNER) {
			VisitInnerOrSemiJoin(join, op);
			return;
		}
	}

	AddRequiredColumns(join.conditions);
	// NULL values in the NULL-extended side can be introduced by this join itself,
	// so columns of that side are not guaranteed to be NULL below the join
	if (join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER) {
		EraseNullRequiredColumns(*join.children[1]);
	}
	if (join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER) {
		EraseNullRequiredColumns(*join.children[0]);
	}
	JoinSidesIgnoreDuplicates(join.join_type, parent.ignores_duplicates, left, right);
	VisitJoinChildren(op, left, right, true);
}

void JoinSimplification::VisitProjection(LogicalProjection &projection, LogicalOperator &op) {
	vector<pair<idx_t, ColumnBinding>> direct_projection_map;
	for (idx_t col_idx = 0; col_idx < projection.expressions.size(); col_idx++) {
		auto &expr = *projection.expressions[col_idx];
		const ColumnBinding binding(projection.table_index, ProjectionIndex(col_idx));
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			AddRequiredColumns(expr);
			continue;
		}

		auto input_binding = expr.Cast<BoundColumnRefExpression>().Binding();
		direct_projection_map.emplace_back(col_idx, input_binding);
		if (null_filtered_columns.find(binding) != null_filtered_columns.end()) {
			null_filtered_columns.insert(input_binding);
		}
		if (null_required_columns.find(binding) != null_required_columns.end()) {
			null_required_columns.insert(input_binding);
		}
	}
	// everything above reads the columns the projection computes, not the columns below it
	ParentInfo info;
	info.ignores_duplicates = parent.ignores_duplicates && !JoinToSemiJoin::HasVolatileExpression(op);
	info.reads_known = true;
	JoinToSemiJoin::AddReadTables(op, info.read_tables);
	VisitChild(op.children[0], std::move(info));

	for (const auto &entry : direct_projection_map) {
		auto col_idx = entry.first;
		auto input_binding = entry.second;
		if (eliminated_null_columns.find(input_binding) == eliminated_null_columns.end()) {
			continue;
		}
		const ColumnBinding output_binding(projection.table_index, ProjectionIndex(col_idx));
		auto type = projection.expressions[col_idx]->GetReturnType();
		projection.expressions[col_idx] = make_uniq<BoundConstantExpression>(Value(type));
		eliminated_null_columns.insert(output_binding);
	}
}

void JoinSimplification::VisitFilter(LogicalFilter &filter, LogicalOperator &op) {
	if (SimplifiesOuterJoins()) {
		// before filter pushdown the filters stay as the region rewrite collected them
		filter.SplitPredicates(filter.expressions);
	}
	for (const auto &expr : filter.expressions) {
		HandleFilterExpression(*expr);
	}
	VisitChild(op.children[0],
	           PassThroughInfo(op, parent.ignores_duplicates && !JoinToSemiJoin::HasVolatileExpression(op)));

	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		ColumnBinding binding;
		if (!IsNullFilter(*filter.expressions[i], binding)) {
			continue;
		}
		if (eliminated_null_columns.find(binding) == eliminated_null_columns.end()) {
			continue;
		}
		filter.expressions.erase_at(i);
		i--;
	}
}

void JoinSimplification::VisitOrder(LogicalOrder &order, LogicalOperator &op) {
	for (const auto &order_node : order.orders) {
		AddRequiredColumns(*order_node.expression);
	}
	VisitChild(op.children[0], UnknownInfo());
}

void JoinSimplification::VisitTopN(LogicalTopN &top_n, LogicalOperator &op) {
	for (const auto &order_node : top_n.orders) {
		AddRequiredColumns(*order_node.expression);
	}
	VisitChildInNewContext(op.children[0], required_columns, UnknownInfo());
}

void JoinSimplification::VisitAggregate(LogicalAggregate &aggregate, LogicalOperator &op) {
	column_binding_set_t child_required_columns;
	for (const auto &expr : aggregate.groups) {
		AddColumnReferences(*expr, child_required_columns);
	}
	for (const auto &expr : aggregate.expressions) {
		AddColumnReferences(*expr, child_required_columns);
	}
	// everything above reads the columns the aggregate computes, not the columns below it
	ParentInfo info;
	info.ignores_duplicates = SelfJoinAggregate::AggregateIgnoresDuplicates(aggregate);
	info.reads_known = true;
	JoinToSemiJoin::AddReadTables(op, info.read_tables);
	VisitChildInNewContext(op.children[0], std::move(child_required_columns), std::move(info));
}

void JoinSimplification::VisitUnsupportedOperator(LogicalOperator &op) {
	if (join_to_semi_join && op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		join_to_semi_join->VisitMaterializedCTE(op);
	}
	// these read their children's columns by position, or hold references this traversal does not follow
	for (auto &child : op.children) {
		column_binding_set_t child_required_columns;
		for (const auto &binding : child->GetColumnBindings()) {
			child_required_columns.insert(binding);
		}
		VisitChildInNewContext(child, std::move(child_required_columns), UnknownInfo());
	}
}

void JoinSimplification::VisitOperator(unique_ptr<LogicalOperator> &op_ptr) {
	if (join_to_semi_join && !parent.in_region && parent.reads_known) {
		// the root of a region of filters, cross products and INNER joins
		while (JoinToSemiJoin::IsRegionNode(*op_ptr) &&
		       join_to_semi_join->RewriteRegion(op_ptr, parent.ignores_duplicates, parent.read_tables)) {
		}
	}
	auto &op = *op_ptr;
	InitializeRequiredColumns(op);

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		VisitComparisonJoin(join, op_ptr);
		return;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// the column sets do not pass through these, the rest does
		bool left = parent.ignores_duplicates;
		bool right = parent.ignores_duplicates;
		if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			JoinSidesIgnoreDuplicates(op.Cast<LogicalJoin>().join_type, parent.ignores_duplicates, left, right);
		}
		VisitJoinChildren(op, left, right, false);
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		VisitProjection(projection, op);
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		VisitFilter(filter, op);
		return;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		VisitOrder(order, op);
		return;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &top_n = op.Cast<LogicalTopN>();
		VisitTopN(top_n, op);
		return;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		VisitChildInNewContext(op.children[0], required_columns, UnknownInfo());
		return;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		VisitAggregate(aggregate, op);
		return;
	}
	default:
		// Passthrough not supported. TODO: could pass through more operators like LOGICAL_UNION
		break;
	}

	VisitUnsupportedOperator(op);
}

} // namespace duckdb
