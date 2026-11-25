#include "duckdb/optimizer/mark_join_simplifier.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/subquery/has_volatile_expressions.hpp"

namespace duckdb {

MarkJoinSimplifier::MarkJoinSimplifier(Optimizer &optimizer) : optimizer(optimizer) {
}

static bool ComparisonInputIsNotNull(NotNullExpressionAnalyzer &analyzer, LogicalOperator &op, const Expression &expr) {
	// These comparisons can produce NULL from nested members.
	if (expr.GetReturnType().id() == LogicalTypeId::TUPLE || expr.GetReturnType().id() == LogicalTypeId::UNION) {
		return false;
	}
	return analyzer.IsNotNull(op, expr);
}

static bool JoinConditionsCannotBeNull(NotNullExpressionAnalyzer &analyzer, LogicalComparisonJoin &join) {
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			return false;
		}
		if (condition.GetComparisonType() == ExpressionType::COMPARE_DISTINCT_FROM ||
		    condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		if (!ComparisonInputIsNotNull(analyzer, *join.children[0], condition.GetLHS()) ||
		    !ComparisonInputIsNotNull(analyzer, *join.children[1], condition.GetRHS())) {
			return false;
		}
	}
	return true;
}

static bool IsMarker(const Expression &expr, ColumnBinding marker) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &column = expr.Cast<BoundColumnRefExpression>();
	return column.Depth() == 0 && column.Binding() == marker;
}

static bool IsMarkerPredicate(const Expression &expr, ColumnBinding marker) {
	if (expr.GetReturnType() != LogicalType::BOOLEAN) {
		return false;
	}
	if (IsMarker(expr, marker)) {
		return true;
	}
	switch (expr.GetExpressionType()) {
	case ExpressionType::VALUE_CONSTANT:
		return true;
	case ExpressionType::OPERATOR_NOT:
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		break;
	default:
		if (!BoundComparisonExpression::IsComparison(expr)) {
			return false;
		}
	}
	bool supported = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { supported &= IsMarkerPredicate(child, marker); });
	return supported;
}

struct MarkerPredicate {
	bool accepts_true = false;
	bool accepts_false = false;
	bool accepts_null = false;

	bool AcceptsOnlyTrue() const {
		return accepts_true && !accepts_false && !accepts_null;
	}

	bool AcceptsOneNonNullValue() const {
		return accepts_true != accepts_false;
	}
};

static MarkerPredicate AnalyzeMarkerPredicate(ClientContext &context, const Expression &expr, ColumnBinding marker) {
	MarkerPredicate result;
	if (IsMarker(expr, marker)) {
		result.accepts_true = true;
		return result;
	}
	if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT &&
	    IsMarker(*expr.Cast<BoundOperatorExpression>().GetChildren()[0], marker)) {
		result.accepts_false = true;
		return result;
	}
	if (!IsMarkerPredicate(expr, marker)) {
		return result;
	}
	auto evaluate = [&](const Value &marker_value, bool &accepted) {
		auto copy = expr.Copy();
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    copy, [&](BoundColumnRefExpression &, unique_ptr<Expression> &ref) {
			    ref = make_uniq<BoundConstantExpression>(marker_value);
		    });
		Value evaluated;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *copy, evaluated)) {
			return false;
		}
		accepted = !evaluated.IsNull() && evaluated.GetValue<bool>();
		return true;
	};
	if (!evaluate(Value::BOOLEAN(true), result.accepts_true) ||
	    !evaluate(Value::BOOLEAN(false), result.accepts_false) ||
	    !evaluate(Value(LogicalType::BOOLEAN), result.accepts_null)) {
		return {};
	}
	return result;
}

static void SimplifyNullSafeSemiJoinConditions(NotNullExpressionAnalyzer &analyzer, LogicalComparisonJoin &join) {
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		// A UNION's active member may be NULL even when the UNION itself is not.
		if (condition.GetLHS().GetReturnType().id() == LogicalTypeId::UNION) {
			continue;
		}
		if (analyzer.IsNotNull(*join.children[0], condition.GetLHS()) ||
		    analyzer.IsNotNull(*join.children[1], condition.GetRHS())) {
			condition.SetComparisonType(ExpressionType::COMPARE_EQUAL);
		}
	}
}

static void ApplyMarkJoinConversion(Optimizer &optimizer, unique_ptr<LogicalOperator> &op, idx_t predicate_index,
                                    const column_binding_map_t<Value> &marker_values,
                                    NotNullExpressionAnalyzer &nullability, BindingReplacementGraph &replacements,
                                    const std::function<void(unique_ptr<LogicalOperator> &)> &rewrite_join) {
	auto &filter = op->Cast<LogicalFilter>();
	auto &original = *filter.children[0];
	original.ResolveOperatorTypes();
	const auto old_bindings = original.GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		auto marker = marker_values.find(old_bindings[i]);
		if (marker != marker_values.end()) {
			expressions.push_back(make_uniq<BoundConstantExpression>(marker->second));
		} else {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(original.types[i], old_bindings[i]));
		}
	}
	auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(expressions));
	BindingReplacementGraph local_replacements;
	auto new_bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		if (!local_replacements.Find(old_bindings[i])) {
			local_replacements.Add(old_bindings[i], new_bindings[i]);
		}
	}
	filter.expressions.erase_at(predicate_index);
	ColumnBindingRewrite::RewriteChild(
	    op, 0, [&](unique_ptr<LogicalOperator> &child, BindingReplacementGraph &child_replacements) {
		    rewrite_join(child);
		    auto &join = child->Cast<LogicalComparisonJoin>();
		    D_ASSERT(join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI);
		    projection->children.push_back(std::move(child));
		    child = std::move(projection);
		    child_replacements.Merge(local_replacements);
		    // CTE nullability analysis needs the replacement attached to the complete plan.
		    if (join.join_type == JoinType::SEMI) {
			    SimplifyNullSafeSemiJoinConditions(nullability, join);
		    }
	    });
	replacements.Merge(local_replacements);
}

bool MarkJoinSimplifier::TrySimplify(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
                                     BindingReplacementGraph &replacements) {
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || !join.duplicate_eliminated_columns.empty()) {
		// Retain unresolved correlation and DELIM semantics (issue #22267).
		return false;
	}
	const ColumnBinding marker(join.mark_index, ProjectionIndex(0));
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		auto predicate = AnalyzeMarkerPredicate(optimizer.context, *filter.expressions[i], marker);
		auto target_type = JoinType::INVALID;
		if (predicate.AcceptsOnlyTrue()) {
			target_type = JoinType::SEMI;
		} else if (predicate.AcceptsOneNonNullValue() && JoinConditionsCannotBeNull(nullability, join)) {
			target_type = predicate.accepts_true ? JoinType::SEMI : JoinType::ANTI;
		}
		if (target_type == JoinType::INVALID) {
			continue;
		}
		// Reordering the converted join can change which rows evaluate volatile expressions.
		if (HasVolatileExpressionsIgnoringError(filter)) {
			return false;
		}
		for (auto &condition : join.conditions) {
			// Reordering can evaluate conditions on rows removed by other joins.
			auto can_throw = condition.IsComparison() ? condition.GetLHS().CanThrow() || condition.GetRHS().CanThrow()
			                                          : condition.GetJoinExpression().CanThrow();
			if (can_throw) {
				return false;
			}
			// MARK tuple inequality uses row equality, unlike ordinary nested inequality.
			if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_NOTEQUAL &&
			    condition.GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE) {
				return false;
			}
		}
		ApplyMarkJoinConversion(optimizer, op, i, {{marker, Value::BOOLEAN(target_type == JoinType::SEMI)}},
		                        nullability, replacements, [&](unique_ptr<LogicalOperator> &child) {
			                        auto &converted_join = child->Cast<LogicalComparisonJoin>();
			                        converted_join.join_type = target_type;
			                        converted_join.mark_types.clear();
		                        });
		return true;
	}
	return false;
}

static bool GetDisjunctiveMarkers(const Expression &expr, column_binding_set_t &markers) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!GetDisjunctiveMarkers(*child, markers)) {
				return false;
			}
		}
		return true;
	}
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF || expr.GetReturnType() != LogicalType::BOOLEAN) {
		return false;
	}
	auto &column = expr.Cast<BoundColumnRefExpression>();
	if (column.Depth() != 0) {
		return false;
	}
	markers.insert(column.Binding());
	return true;
}

static bool CanDiscardMarkers(const Expression &expr, const column_binding_set_t &markers) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &column = expr.Cast<BoundColumnRefExpression>();
		return column.Depth() == 0 && !markers.count(column.Binding());
	}
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_SUBQUERY:
		return false;
	default:
		break;
	}
	bool result = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { result = result && CanDiscardMarkers(child, markers); });
	return result;
}

static bool CanDiscardMarkers(const LogicalOperator &op, const column_binding_set_t &markers) {
	bool result = true;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](const unique_ptr<Expression> *expr) { result = result && CanDiscardMarkers(**expr, markers); });
	return result;
}

static bool MarkersAreUnused(const LogicalFilter &filter, idx_t predicate_index,
                             const vector<reference<LogicalOperator>> &ancestors, const column_binding_set_t &markers) {
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		if (i != predicate_index && !CanDiscardMarkers(*filter.expressions[i], markers)) {
			return false;
		}
	}
	for (idx_t i = ancestors.size(); i > 0; i--) {
		auto &ancestor = ancestors[i - 1].get();
		if (!CanDiscardMarkers(ancestor, markers)) {
			return false;
		}
		switch (ancestor.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			// Any use above this binding boundary must first read a marker in its expressions.
			return true;
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_WINDOW:
			break;
		default:
			// Do not infer liveness across positional consumers or dependent sibling subtrees.
			return false;
		}
		if (ancestor.children.size() != 1) {
			return false;
		}
	}
	return false;
}

static bool BuildIsIndependent(const LogicalOperator &op, const column_binding_set_t &markers) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		return false;
	default:
		break;
	}
	if (!CanDiscardMarkers(op, markers)) {
		return false;
	}
	for (auto &child : op.children) {
		if (!BuildIsIndependent(*child, markers)) {
			return false;
		}
	}
	return true;
}

static void PushDownMarkJoins(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		PushDownMarkJoins(child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &mark = op->Cast<LogicalComparisonJoin>();
	if (mark.join_type != JoinType::MARK || !mark.duplicate_eliminated_columns.empty() || mark.HasProjectionMap() ||
	    mark.filter_pushdown || HasVolatileExpressionsIgnoringError(mark)) {
		return;
	}
	unordered_set<TableIndex> probe_tables;
	for (auto &condition : mark.conditions) {
		// Moving below an INNER join can evaluate the probe on rows that the join would discard.
		if (!condition.IsComparison() || condition.GetLHS().CanThrow() || condition.GetRHS().CanThrow() ||
		    !CanDiscardMarkers(condition.GetLHS(), {}) || !CanDiscardMarkers(condition.GetRHS(), {})) {
			return;
		}
		LogicalJoin::GetExpressionBindings(condition.GetLHS(), probe_tables);
	}
	if (probe_tables.empty()) {
		return;
	}
	const auto probe_bindings = mark.children[0]->GetColumnBindings();
	if (!BuildIsIndependent(*mark.children[1], column_binding_set_t(probe_bindings.begin(), probe_bindings.end()))) {
		return;
	}
	auto target = &mark.children[0];
	while (true) {
		auto &input = **target;
		if (input.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = input.Cast<LogicalComparisonJoin>();
			if (join.join_type != JoinType::INNER || join.HasProjectionMap() || join.filter_pushdown) {
				break;
			}
		} else if (input.type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			break;
		}
		unordered_set<TableIndex> left_tables, right_tables;
		LogicalJoin::GetTableReferences(*input.children[0], left_tables);
		LogicalJoin::GetTableReferences(*input.children[1], right_tables);
		auto side = JoinSide::GetJoinSide(probe_tables, left_tables, right_tables);
		if (side != JoinSide::LEFT && side != JoinSide::RIGHT) {
			break;
		}
		target = &input.children[side == JoinSide::LEFT ? 0 : 1];
	}
	if (target == &mark.children[0]) {
		return;
	}
	const auto old_bindings = mark.GetColumnBindings();
	// MARK(A JOIN B, keys from B) becomes A JOIN MARK(B), with the same marker and build input.
	auto input = std::move(mark.children[0]);
	mark.children[0] = std::move(*target);
	mark.has_estimated_cardinality = false;
	*target = std::move(op);
	op = std::move(input);

	const auto new_bindings = op->GetColumnBindings();
	if (old_bindings != new_bindings) {
		// Keep positional consumers intact when the marker moved into the left input.
		vector<ProjectionIndex> projection_map;
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			projection_map.emplace_back(i);
		}
		LogicalOperatorVisitor::RemapProjectionMap(projection_map, old_bindings, new_bindings);
		op = LogicalJoin::PromoteJoinSide(std::move(op), std::move(projection_map));
	}
}

static bool SameMarkJoinKeys(const LogicalComparisonJoin &join, const LogicalComparisonJoin &other) {
	if (join.conditions.empty() || join.conditions.size() != other.conditions.size()) {
		return false;
	}
	for (idx_t i = 0; i < join.conditions.size(); i++) {
		auto &condition = join.conditions[i];
		auto &other_condition = other.conditions[i];
		if (!condition.IsComparison() || !other_condition.IsComparison()) {
			return false;
		}
		auto comparison = condition.GetComparisonType();
		if ((comparison != ExpressionType::COMPARE_EQUAL && comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
		    comparison != other_condition.GetComparisonType() || !condition.GetLHS().Equals(other_condition.GetLHS()) ||
		    !condition.GetLHS().GetReturnType().EqualsIncludingCollation(other_condition.GetLHS().GetReturnType()) ||
		    !condition.GetRHS().GetReturnType().EqualsIncludingCollation(other_condition.GetRHS().GetReturnType()) ||
		    condition.GetLHS().CanThrow() || condition.GetRHS().CanThrow()) {
			return false;
		}
	}
	return true;
}

bool MarkJoinSimplifier::TrySimplifyDisjunction(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
                                                const vector<reference<LogicalOperator>> &ancestors,
                                                BindingReplacementGraph &replacements) {
	auto &filter = op->Cast<LogicalFilter>();
	for (idx_t predicate_index = 0; predicate_index < filter.expressions.size(); predicate_index++) {
		auto &predicate = *filter.expressions[predicate_index];
		if (predicate.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			continue;
		}
		column_binding_set_t markers;
		if (!GetDisjunctiveMarkers(predicate, markers) || markers.size() < 2 ||
		    !MarkersAreUnused(filter, predicate_index, ancestors, markers)) {
			continue;
		}
		vector<reference<LogicalComparisonJoin>> joins;
		auto remaining = markers;
		optional_ptr<LogicalOperator> probe = filter.children[0].get();
		while (probe->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = probe->Cast<LogicalComparisonJoin>();
			if (join.join_type != JoinType::MARK || !join.duplicate_eliminated_columns.empty() ||
			    !remaining.erase(ColumnBinding(join.mark_index, ProjectionIndex(0))) ||
			    !SameMarkJoinKeys(join, joins.empty() ? join : joins[0].get()) || !CanDiscardMarkers(join, markers) ||
			    !BuildIsIndependent(*join.children[1], markers)) {
				break;
			}
			joins.push_back(join);
			probe = join.children[0].get();
			if (remaining.empty()) {
				break;
			}
		}
		if (joins.size() != markers.size() || HasVolatileExpressionsIgnoringError(filter)) {
			continue;
		}
		column_binding_map_t<Value> marker_values;
		for (auto &marker : markers) {
			// The individual marks are dead, but retain their slots until ordinary column pruning.
			marker_values.emplace(marker, Value(LogicalType::BOOLEAN));
		}
		ApplyMarkJoinConversion(
		    optimizer, op, predicate_index, marker_values, nullability, replacements,
		    [&](unique_ptr<LogicalOperator> &child) {
			    auto &top = joins[0].get();
			    const auto union_index = optimizer.binder.GenerateTableIndex();
			    auto semi = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
			    for (idx_t i = 0; i < top.conditions.size(); i++) {
				    auto &condition = top.conditions[i];
				    auto key = make_uniq<BoundColumnRefExpression>(condition.GetRHS().GetReturnType(),
				                                                   ColumnBinding(union_index, ProjectionIndex(i)));
				    semi->conditions.emplace_back(condition.GetLHS().Copy(), std::move(key),
				                                  condition.GetComparisonType());
			    }
			    vector<unique_ptr<LogicalOperator>> builds;
			    for (auto &join_ref : joins) {
				    auto &join = join_ref.get();
				    vector<unique_ptr<Expression>> keys;
				    for (auto &condition : join.conditions) {
					    keys.push_back(condition.GetRHS().Copy());
				    }
				    auto build = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(keys));
				    build->children.push_back(std::move(join.children[1]));
				    builds.push_back(std::move(build));
			    }
			    auto union_op = make_uniq<LogicalSetOperation>(union_index, semi->conditions.size(), std::move(builds),
			                                                   LogicalOperatorType::LOGICAL_UNION, true);
			    semi->children.push_back(std::move(joins.back().get().children[0]));
			    semi->children.push_back(std::move(union_op));
			    child = std::move(semi);
		    });
		return true;
	}
	return false;
}

void MarkJoinSimplifier::Rewrite(unique_ptr<LogicalOperator> &op, NotNullExpressionAnalyzer &nullability,
                                 BindingReplacementGraph &replacements, vector<reference<LogicalOperator>> &ancestors) {
	ancestors.push_back(*op);
	for (auto &child : op->children) {
		Rewrite(child, nullability, replacements, ancestors);
	}
	ancestors.pop_back();
	// Visit original consumers before adding their enclosing conversion's replacement edges.
	ColumnBindingRewrite::ApplyToOperatorBindings(*op, replacements);
	if (op->type == LogicalOperatorType::LOGICAL_FILTER &&
	    (TrySimplify(op, nullability, replacements) ||
	     TrySimplifyDisjunction(op, nullability, ancestors, replacements))) {
		auto &filter = op->Cast<LogicalFilter>();
		if (filter.expressions.empty() && !filter.HasProjectionMap()) {
			op = std::move(filter.children[0]);
		}
	}
}

void MarkJoinSimplifier::Optimize(unique_ptr<LogicalOperator> &plan) {
	NotNullExpressionAnalyzer nullability(optimizer.context, *plan);
	BindingReplacementGraph replacements;
	vector<reference<LogicalOperator>> ancestors;
	Rewrite(plan, nullability, replacements, ancestors);
	// First simplify filtering MARKs; relocating them can hide the filter/chain patterns above.
	PushDownMarkJoins(plan);
}

} // namespace duckdb
