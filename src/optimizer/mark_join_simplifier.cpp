#include "duckdb/optimizer/mark_join_simplifier.hpp"

#include "duckdb/common/assert.hpp"
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
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/subquery/has_volatile_expressions.hpp"

namespace duckdb {

MarkJoinSimplifier::MarkJoinSimplifier(Optimizer &optimizer) : optimizer(optimizer) {
}

static bool IsConstantInputComparisonOptimizable(const JoinCondition &condition) {
	if (!condition.IsComparison() || condition.GetRHS().IsVolatile()) {
		return false;
	}
	// A NULL-only hash build can skip evaluating the probe expression.
	if (condition.GetLHS().IsVolatile() || condition.GetLHS().CanThrow()) {
		return false;
	}

	const auto comparison_type = condition.GetComparisonType();
	if ((condition.GetLHS().GetReturnType().id() == LogicalTypeId::TUPLE ||
	     condition.GetRHS().GetReturnType().id() == LogicalTypeId::TUPLE) &&
	    comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
		return false;
	}

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

bool MarkJoinSimplifier::TrySimplifyConstantInput(unique_ptr<LogicalOperator> &op,
                                                  BindingReplacementGraph &replacements) {
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}

	auto &join = op->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || join.HasArbitraryConditions() || join.conditions.size() != 1 ||
	    !IsConstantInputComparisonOptimizable(join.conditions[0])) {
		return false;
	}

	D_ASSERT(join.children.size() == 2);
	auto &right_plan = *join.children[1];
	if (right_plan.type != LogicalOperatorType::LOGICAL_PROJECTION || right_plan.children.size() != 1 ||
	    right_plan.children[0]->type != LogicalOperatorType::LOGICAL_DUMMY_SCAN || right_plan.expressions.size() != 1) {
		return false;
	}

	const auto &constant_expression = *right_plan.expressions[0];
	if (!constant_expression.IsFoldable() || constant_expression.HasParameter() || constant_expression.IsVolatile() ||
	    constant_expression.CanThrow()) {
		return false;
	}

	const auto &right_bindings = right_plan.GetColumnBindings();
	if (right_bindings.size() != 1) {
		return false;
	}

	Value constant;
	if (!ExpressionExecutor::TryEvaluateScalar(optimizer.context, constant_expression, constant)) {
		return false;
	}

	const auto &condition = join.conditions[0];
	const auto &left_bindings = join.children[0]->GetColumnBindings();
	bool valid_left_expression = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(condition.GetLHS(), [&](const auto &ref) {
		if (ref.Depth() != 0 ||
		    std::find(left_bindings.begin(), left_bindings.end(), ref.Binding()) == left_bindings.end()) {
			valid_left_expression = false;
		}
	});

	const auto right_binding = right_bindings[0];
	bool valid_right_expression = true;
	bool found_right_binding = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(condition.GetRHS(), [&](const auto &ref) {
		if (ref.Depth() != 0 || ref.Binding() != right_binding) {
			valid_right_expression = false;
		}
		found_right_binding |= ref.Binding() == right_binding;
	});

	if (!valid_left_expression || !valid_right_expression || !found_right_binding) {
		return false;
	}

	const auto old_bindings = join.GetColumnBindings();
	const auto old_types = join.types;
	if (old_bindings.size() != old_types.size() || old_bindings.empty() ||
	    old_bindings.back() != ColumnBinding(join.mark_index, ProjectionIndex(0)) ||
	    old_types.back() != LogicalType::BOOLEAN) {
		return false;
	}

	for (idx_t i = 0; i + 1 < old_bindings.size(); ++i) {
		if (std::find(left_bindings.begin(), left_bindings.end(), old_bindings[i]) == left_bindings.end()) {
			return false;
		}
	}

	auto comparison_right = condition.GetRHS().Copy();
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    comparison_right, [&right_binding, &constant](auto &ref, auto &slot) {
		    if (ref.Binding() == right_binding) {
			    slot = make_uniq<BoundConstantExpression>(constant);
		    }
	    });

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(old_bindings.size());
	for (idx_t i = 0; i + 1 < old_bindings.size(); ++i) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(old_types[i], old_bindings[i]));
	}
	expressions.push_back(BoundComparisonExpression::Create(condition.GetComparisonType(), condition.GetLHS().Copy(),
	                                                        std::move(comparison_right)));
	auto projection = make_uniq<LogicalProjection>(optimizer.binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(join.children[0]));
	projection->ResolveOperatorTypes();
	const auto new_bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < old_bindings.size(); ++i) {
		replacements.Add(old_bindings[i], new_bindings[i]);
	}
	op = std::move(projection);
	return true;
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
                                 BindingReplacementGraph &replacements, vector<reference<LogicalOperator>> &ancestors,
                                 bool constant_input_only) {
	ancestors.push_back(*op);
	for (idx_t child_idx = 0; child_idx < op->children.size(); ++child_idx) {
		ColumnBindingRewrite::RewriteChild(
		    op, child_idx,
		    [this, &nullability, &replacements, &ancestors,
		     constant_input_only](unique_ptr<LogicalOperator> &child, BindingReplacementGraph &child_replacements) {
			    Rewrite(child, nullability, child_replacements, ancestors, constant_input_only);
			    replacements.Merge(child_replacements);
		    });
	}
	ancestors.pop_back();
	// Visit original consumers before adding their enclosing conversion's replacement edges.
	ColumnBindingRewrite::ApplyToOperatorBindings(*op, replacements);
	if (constant_input_only) {
		TrySimplifyConstantInput(op, replacements);
	} else if (op->type == LogicalOperatorType::LOGICAL_FILTER &&
	           (TrySimplify(op, nullability, replacements) ||
	            TrySimplifyDisjunction(op, nullability, ancestors, replacements))) {
		auto &filter = op->Cast<LogicalFilter>();
		if (filter.expressions.empty() && !filter.HasProjectionMap()) {
			op = std::move(filter.children[0]);
		}
	}
}

void MarkJoinSimplifier::OptimizeConstantInput(unique_ptr<LogicalOperator> &plan) {
	plan->ResolveOperatorTypes();
	NotNullExpressionAnalyzer nullability(optimizer.context, *plan);
	BindingReplacementGraph replacements;
	vector<reference<LogicalOperator>> ancestors;
	Rewrite(plan, nullability, replacements, ancestors, true);
}

void MarkJoinSimplifier::Optimize(unique_ptr<LogicalOperator> &plan) {
	NotNullExpressionAnalyzer nullability(optimizer.context, *plan);
	BindingReplacementGraph replacements;
	vector<reference<LogicalOperator>> ancestors;
	Rewrite(plan, nullability, replacements, ancestors, false);
}

} // namespace duckdb
