#include "duckdb/cascade/apply_decorrelation.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

ApplyDecorrelator::ApplyDecorrelator(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

//! True if any sub-expression references a column of the correlation domain.
static bool ReferencesCorrelation(const Expression &expr, const CorrelatedColumns &correlated) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (idx_t i = 0; i < correlated.size(); i++) {
			    if (correlated[i].binding == colref.Binding()) {
				    found = true;
				    return;
			    }
		    }
	    });
	return found;
}

//! True if any sub-expression reads a column of the sub-query's own body, i.e. a
//! column that is not one of the parameters handed in from the outer query.
static bool ReferencesInner(const Expression &expr, const CorrelatedColumns &correlated) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (idx_t i = 0; i < correlated.size(); i++) {
			    if (correlated[i].binding == colref.Binding()) {
				    return;
			    }
		    }
		    found = true;
	    });
	return found;
}

//! Move every column reference one scope closer to the query the sub-query is being
//! spliced into: a reference to the enclosing query is depth 1 while it is inside the
//! sub-query and depth 0 once the sub-query is gone. A depth 0 reference is already
//! local and stays put, and a reference to a further outer query - made by a
//! sub-query nested inside this one - shifts down with it.
static void DecrementCorrelationDepth(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.Depth() > 0) {
			colref.DepthMutable()--;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [](Expression &child) { DecrementCorrelationDepth(child); });
}

static void DecrementCorrelationDepth(LogicalOperator &op) {
	for (auto &expr : op.expressions) {
		DecrementCorrelationDepth(*expr);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &condition = op.Cast<LogicalDependentJoin>().condition;
		if (condition) {
			DecrementCorrelationDepth(*condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			DecrementCorrelationDepth(*condition.LeftReference());
			DecrementCorrelationDepth(*condition.RightReference());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			DecrementCorrelationDepth(*condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// The grouping expressions live beside op.expressions, not in it.
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			DecrementCorrelationDepth(*group);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		DecrementCorrelationDepth(*child);
	}
}

static bool ListReferencesCorrelation(const vector<unique_ptr<Expression>> &exprs,
                                      const CorrelatedColumns &correlated) {
	for (auto &expr : exprs) {
		if (ReferencesCorrelation(*expr, correlated)) {
			return true;
		}
	}
	return false;
}

//! True if the sub-tree still mentions a correlated column once the filter
//! predicates above the correlation point have been extracted.
static bool SubtreeReferencesCorrelation(const LogicalOperator &op, const CorrelatedColumns &correlated) {
	if (ListReferencesCorrelation(op.expressions, correlated)) {
		return true;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				// a single-expression condition exposes no accessor: assume the worst
				return true;
			}
			if (ReferencesCorrelation(condition.GetLHS(), correlated) ||
			    ReferencesCorrelation(condition.GetRHS(), correlated)) {
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition && ReferencesCorrelation(*condition, correlated)) {
			return true;
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		if (SubtreeReferencesCorrelation(*child, correlated)) {
			return true;
		}
	}
	return false;
}

//! Move correlated predicates out of the filters above the correlation point.
//! Only LogicalFilter and LogicalProjection are looked through: both preserve
//! rows, so a predicate lifted past them keeps its meaning. Any other operator
//! ends the walk, leaving whatever it holds to be reported by the caller.
static unique_ptr<LogicalOperator> ExtractCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                               const CorrelatedColumns &correlated,
                                                               vector<unique_ptr<Expression>> &extracted) {
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		filter.SplitPredicates();
		vector<unique_ptr<Expression>> local;
		for (auto &expr : op->expressions) {
			if (ReferencesCorrelation(*expr, correlated)) {
				extracted.push_back(std::move(expr));
			} else {
				local.push_back(std::move(expr));
			}
		}
		op->children[0] = ExtractCorrelatedPredicates(std::move(op->children[0]), correlated, extracted);
		if (local.empty()) {
			// nothing local left: the filter itself disappears
			return std::move(op->children[0]);
		}
		op->expressions = std::move(local);
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		op->children[0] = ExtractCorrelatedPredicates(std::move(op->children[0]), correlated, extracted);
		return op;
	}
	return op;
}

//! A column of the right sub-tree that a lifted predicate needs to see.
struct NeededColumn {
	ColumnBinding binding;
	LogicalType type;
};

//! Columns referenced by the lifted predicates that live on the right side.
static vector<NeededColumn> CollectRightColumns(const vector<unique_ptr<Expression>> &predicates,
                                                const CorrelatedColumns &correlated) {
	vector<NeededColumn> result;
	for (auto &predicate : predicates) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *predicate, [&](const BoundColumnRefExpression &colref) {
			    for (idx_t i = 0; i < correlated.size(); i++) {
				    if (correlated[i].binding == colref.Binding()) {
					    return;
				    }
			    }
			    for (auto &entry : result) {
				    if (entry.binding == colref.Binding()) {
					    return;
				    }
			    }
			    LogicalType type = colref.GetReturnType();
			    result.push_back(NeededColumn {colref.Binding(), std::move(type)});
		    });
	}
	return result;
}

//! The right sub-tree must output every column a lifted predicate references,
//! because the join condition is evaluated above it. Append whatever the top
//! projection does not already expose, and report the new bindings.
static void ExposeRightColumns(LogicalOperator &right, const vector<NeededColumn> &needed,
                               vector<std::pair<ColumnBinding, ColumnBinding>> &mapping) {
	if (right.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		throw NotImplementedException(
		    "cascade: Apply elimination needs a projection on top of the subquery to expose the "
		    "correlated columns, but found a different operator");
	}
	auto &projection = right.Cast<LogicalProjection>();
	// A column can only be appended to this projection if the projection's own child
	// already produces it. When it does not - the column sits below a second
	// projection - these rules cannot expose it, and saying so beats emitting a plan
	// whose bindings do not resolve.
	if (!right.children.empty()) {
		auto child_bindings = right.children[0]->GetColumnBindings();
		for (auto &col : needed) {
			bool visible = false;
			for (auto &binding : child_bindings) {
				if (binding == col.binding) {
					visible = true;
					break;
				}
			}
			if (!visible) {
				throw NotImplementedException(
				    "cascade: a lifted correlated predicate names a column that the sub-query's own "
				    "projection does not expose");
			}
		}
	}
	// projections that already pass a needed column through keep their binding
	for (idx_t i = 0; i < projection.expressions.size(); i++) {
		auto &expr = projection.expressions[i];
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		for (auto &col : needed) {
			if (col.binding == colref.Binding()) {
				mapping.emplace_back(col.binding, ColumnBinding(projection.table_index, ProjectionIndex(i)));
			}
		}
	}
	for (auto &col : needed) {
		bool exposed = false;
		for (auto &entry : mapping) {
			if (entry.first == col.binding) {
				exposed = true;
				break;
			}
		}
		if (exposed) {
			continue;
		}
		auto position = projection.expressions.size();
		projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		mapping.emplace_back(col.binding, ColumnBinding(projection.table_index, ProjectionIndex(position)));
	}
}

static void RewriteBindings(unique_ptr<Expression> &expr,
                            const vector<std::pair<ColumnBinding, ColumnBinding>> &mapping);

//! Identities (3) and (4) of Galindo-Legaria & Joshi as a framework rather than a
//! single step. A correlated predicate travels up through the row-preserving operators
//! of the sub-query's body until it reaches the Apply, where it becomes a join
//! condition:
//!
//!   (3)  R A (sigma_p E) = sigma_p (R A E)          - a filter contributes its own
//!        predicates and disappears when nothing is left of it;
//!   (4)  R A (pi_v E)    = pi_{v + cols(R)}(R A E)  - a projection has to expose
//!        every column those predicates read, so that they keep being evaluable
//!        above it.
//!
//! The walk is bottom-up on purpose. A predicate lifted from *below* a projection is
//! re-expressed through it, which is what flattens a derived table that has a
//! correlated filter inside it - not just a correlated filter at the very top, where
//! one exposure is enough.
static unique_ptr<LogicalOperator> LiftCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                            const CorrelatedColumns &correlated,
                                                            vector<unique_ptr<Expression>> &pending) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		op->children[0] = LiftCorrelatedPredicates(std::move(op->children[0]), correlated, pending);
		if (!pending.empty()) {
			auto needed = CollectRightColumns(pending, correlated);
			vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
			ExposeRightColumns(*op, needed, mapping);
			for (auto &predicate : pending) {
				RewriteBindings(predicate, mapping);
			}
		}
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		op->children[0] = LiftCorrelatedPredicates(std::move(op->children[0]), correlated, pending);
		auto &filter = op->Cast<LogicalFilter>();
		filter.SplitPredicates();
		vector<unique_ptr<Expression>> local;
		for (auto &expr : filter.expressions) {
			if (ReferencesCorrelation(*expr, correlated)) {
				pending.push_back(std::move(expr));
			} else {
				local.push_back(std::move(expr));
			}
		}
		if (local.empty()) {
			// nothing is left to evaluate here: the filter itself disappears
			return std::move(filter.children[0]);
		}
		filter.expressions = std::move(local);
		return op;
	}
	// Anything else - an aggregate, a join, a set operation - does not preserve the
	// bindings a predicate would have to be re-expressed through, so the walk stops.
	// The correlation that is left is reported by the caller.
	return op;
}

//! Point the lifted predicates at the bindings the right sub-tree now exposes.
static void RewriteBindings(unique_ptr<Expression> &expr,
                            const vector<std::pair<ColumnBinding, ColumnBinding>> &mapping) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : mapping) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

//! Follow a binding through the exposure mapping.
static ColumnBinding MapBinding(const ColumnBinding &binding,
                                const vector<std::pair<ColumnBinding, ColumnBinding>> &mapping) {
	for (auto &entry : mapping) {
		if (entry.first == binding) {
			return entry.second;
		}
	}
	return binding;
}

//! Turn an extracted correlated predicate into a join condition. Comparisons
//! become proper join conditions; anything else is kept as a single-expression
//! condition, which DuckDB resolves over the combined scope.
//!
//! null_safe upgrades an equality to IS NOT DISTINCT FROM, which is what makes a
//! marker two-valued: with plain equality a NULL on either side leaves the
//! comparison unknown, so a MARK join could hand back NULL.
static void AddJoinCondition(LogicalComparisonJoin &join, unique_ptr<Expression> predicate,
                             const CorrelatedColumns &correlated, bool null_safe) {
	if (!BoundComparisonExpression::IsComparison(*predicate) ||
	    predicate->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		join.conditions.emplace_back(std::move(predicate));
		return;
	}
	auto &comparison = predicate->Cast<BoundFunctionExpression>();
	auto lhs = std::move(BoundComparisonExpression::LeftMutable(comparison));
	auto rhs = std::move(BoundComparisonExpression::RightMutable(comparison));
	// The binding resolver resolves a condition's left expression against the
	// left child and its right expression against the right child, so the side
	// that comes from the outer sub-tree has to be written on the left.
	if (!ReferencesCorrelation(*lhs, correlated) && ReferencesCorrelation(*rhs, correlated)) {
		std::swap(lhs, rhs);
		BoundComparisonExpression::FlipType(comparison);
	}
	if (null_safe && comparison.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
		BoundComparisonExpression::SetType(comparison, ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
	join.conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateApply(unique_ptr<LogicalOperator> op, BindingExport &exports) {
	auto &apply = op->Cast<LogicalDependentJoin>();
	const auto &correlated = apply.correlated_columns;
	auto join_type = apply.join_type;
	auto mark_index = apply.mark_index;
	auto any_join = apply.any_join;

	vector<unique_ptr<Expression>> extracted;
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	// The sub-query's plan is about to be spliced into the enclosing query, so every
	// reference it makes to that query moves one scope closer. Leaving them at depth 1
	// happens to compute the right answer - once the plan is flat the resolver finds
	// the binding - but the optimizer is entitled to assume a flattened plan has no
	// depth left: FilterPushdown::IsVolatile asserts it before pushing a filter
	// through a projection, and DuckDB's own flattening removes the depth as it goes.
	// Without this, TPC-H Q17 and Q20 plan fine without the optimizer and fail with it.
	DecrementCorrelationDepth(*right);

	// Rule 3: a correlated scalar subquery. Dispatched before the generic lift,
	// because its correlated predicate sits below the aggregate and the generic
	// walk deliberately refuses to look through one.
	if (join_type == JoinType::SINGLE) {
		return DecorrelateScalar(std::move(left), std::move(right), correlated, exports);
	}

	// Identities (3) and (4), applied level by level through the sub-query's body.
	right = LiftCorrelatedPredicates(std::move(right), correlated, extracted);

	// Correlation used in a shape we cannot lift into a join condition yet.
	// Fail loudly rather than emitting a plan that means something else.
	if (SubtreeReferencesCorrelation(*right, correlated)) {
		throw NotImplementedException(
		    "cascade: Apply elimination does not handle this correlated subquery shape yet "
		    "(correlated column used below a non row-preserving operator)");
	}

	// Rule 1: no correlation and an ordinary join type -> cross product.
	if (correlated.empty() && extracted.empty() && !any_join &&
	    (join_type == JoinType::INNER || join_type == JoinType::LEFT || join_type == JoinType::RIGHT ||
	     join_type == JoinType::OUTER)) {
		return make_uniq<LogicalCrossProduct>(std::move(left), std::move(right));
	}

	if (extracted.empty()) {
		throw NotImplementedException(
		    "cascade: Apply elimination without a correlation predicate is not implemented yet "
		    "(uncorrelated semi/anti/mark subquery)");
	}

	// Rule 2 covers the semi/anti/mark family. Only those may have their right
	// sub-tree widened: their output is the left side (plus the mark column), so
	// extra columns on the right stay invisible to the parent.
	if (join_type != JoinType::SEMI && join_type != JoinType::ANTI && join_type != JoinType::MARK) {
		throw NotImplementedException(
		    "cascade: Apply elimination with a correlated predicate is only implemented for "
		    "semi/anti/mark joins so far");
	}
	// x = ANY(subquery) / ALL(subquery) carries a second condition comparing an
	// outer expression with the subquery's output. It becomes one more join
	// condition, and - unlike EXISTS - it keeps its three-valued marker, so it
	// must not receive the NULL-stripping treatment below.
	auto any_condition = std::move(apply.condition);

	// The correlation comparison is always made NULL-safe, and the right side is
	// stripped of NULLs in the correlated columns. Together they keep the
	// correlation behaving like the equality the user wrote (a NULL on either
	// side is not a match) while leaving the marker itself free of the unknown
	// that an equality against NULL would otherwise introduce. Whether the marker
	// ends up two-valued then follows from the remaining conditions: EXISTS has
	// only these, an ANY/IN comparison adds one that can still be NULL.

	// The predicates were re-expressed through every projection they crossed, so the
	// columns they name are the ones the right sub-tree exposes now.
	auto needed = CollectRightColumns(extracted, correlated);

	// EXISTS / NOT EXISTS carry ANY semantics: a NULL comparison counts as "no
	// match", not as "unknown". Dropping the right-side rows whose correlated
	// column is NULL removes the unknown case outright, so a plain MARK join then
	// produces the two-valued marker these subqueries need.
	if (!needed.empty()) {
		vector<unique_ptr<Expression>> not_null;
		for (auto &col : needed) {
			auto colref = make_uniq<BoundColumnRefExpression>(col.type, col.binding);
			auto is_not_null =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null->GetChildrenMutable().push_back(std::move(colref));
			not_null.push_back(std::move(is_not_null));
		}
		if (!not_null.empty()) {
			auto null_filter = make_uniq<LogicalFilter>();
			null_filter->expressions = std::move(not_null);
			null_filter->children.push_back(std::move(right));
			right = std::move(null_filter);
		}
	}

	auto join = make_uniq<LogicalComparisonJoin>(join_type);
	join->mark_index = mark_index;
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	// A predicate inside the sub-query's body is a WHERE condition: only TRUE lets a row
	// through, so an UNKNOWN behaves as FALSE. An equality keeps its shape - it is the
	// join's hash key, and null_safe makes it two-valued, so it contributes nothing to
	// the marker's third value. Everything else is not a hash key anyway, and is wrapped
	// so that UNKNOWN reads as FALSE.
	vector<unique_ptr<Expression>> body_conditions;
	for (auto &predicate : extracted) {
		bool equality = BoundComparisonExpression::IsComparison(*predicate) &&
		                predicate->GetExpressionType() == ExpressionType::COMPARE_EQUAL;
		if (!equality) {
			auto coalesce =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::BOOLEAN);
			coalesce->GetChildrenMutable().push_back(std::move(predicate));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
			predicate = std::move(coalesce);
		}
		if (!any_condition || equality) {
			AddJoinCondition(*join, std::move(predicate), correlated, true);
		} else {
			body_conditions.push_back(std::move(predicate));
		}
	}
	if (any_condition) {
		// An IN/ANY marker is three-valued in one specific way: a NULL in the comparison
		// is unknown, but a row the sub-query's *body* excluded takes no part in it at
		// all. A correlated body predicate cannot be reproduced by a join condition -
		// the marker would either see the body's unknown as its own, or lose it - so this
		// shape is reported instead of answered with the wrong rows. (The paper's own
		// route for booleans is to turn the sub-query into a scalar count first, which
		// brings the body predicate along as the aggregate's input.)
		if (!body_conditions.empty()) {
			throw NotImplementedException(
			    "cascade: a correlated predicate inside an IN/ANY sub-query's body is not implemented yet");
		}
		// It compares an outer expression with the subquery's own output, which the
		// projection already exposes, so it needs no column exposure of its own.
		AddJoinCondition(*join, std::move(any_condition), correlated, false);
	}
	return std::move(join);
}

//! A column of the sub-tree together with the binding the join can reach it by.
struct ExposedColumn {
	ColumnBinding original;
	ColumnBinding visible;
	LogicalType type;
};

//! Point an operator's own expressions at the bindings its child now exposes.
static void RewriteOperatorBindings(LogicalOperator &op, const BindingExport &exports);

//! The GroupBy a scalar sub-query's correlation can be hiding under: a sub-query like
//! `select sum(x) from (select a, max(b) as x from s where s.a = t.a group by a) q`
//! aggregates twice, and the correlated predicate belongs to the inner GroupBy.
static LogicalAggregate *FindNestedGroupedAggregate(LogicalOperator &op) {
	auto current = &op;
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION && current->children.size() == 1) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}
	auto &aggregate = current->Cast<LogicalAggregate>();
	if (aggregate.groups.empty()) {
		return nullptr;
	}
	return &aggregate;
}

//! Identity (8) of Galindo-Legaria & Joshi, for the scalar case where the correlated
//! predicate sits below a GroupBy of the sub-query's own:
//!     R A_x (G_{A,F} E)  =  G_{A ∪ columns(R), F}( R A_x E )
//! The sub-query's own GroupBy cannot be aggregated around - the correlation has to be
//! removed where it lives - so the Apply moves below it and the outer columns join its
//! grouping. Each original group then splits by outer row, and `F` over such a
//! sub-group is exactly the aggregate over the rows that outer row matched.
//!
//! Three details make that work for a *scalar* sub-query, and each of them was a bug
//! before it was a design:
//!
//!  * The outer side is deduplicated first. The original Apply evaluates the sub-query
//!    once per distinct correlation value, so two identical outer rows share one result;
//!    grouping the joined rows by `columns(R)` instead folds them together and, for a
//!    combination like `sum(sum(x))`, counts their rows twice.
//!  * The join under the GroupBy is an inner join. A left outer join pads an outer
//!    value with no match with a NULL row, and the GroupBy materialises that padding
//!    into a group of its own - so the derived table above it would hold one row where
//!    SQL says it is empty (making `count(*)` return 1 instead of 0).
//!  * The multiplicity is handed back afterwards, by joining the aggregated sub-query
//!    to the *original* outer rows on those columns (NULL-safe, so an outer row whose
//!    correlation column is NULL still finds its group). An outer value with no match
//!    is padded, which is exactly what an empty aggregate input returns - except for
//!    count, which pi_c repairs with a constant.
unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateNestedScalar(
    unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right, const vector<LogicalOperator *> &projections,
    LogicalAggregate &top, LogicalAggregate &nested, vector<unique_ptr<Expression>> &extracted,
    const CorrelatedColumns &correlated, BindingExport &exports, const ColumnBinding &value_binding) {
	auto needed = CollectRightColumns(extracted, correlated);
	if (needed.empty()) {
		throw NotImplementedException("cascade: a correlated scalar subquery with no sub-query column");
	}

	auto left_bindings = left->GetColumnBindings();
	left->ResolveOperatorTypes();
	if (left->types.size() != left_bindings.size()) {
		throw NotImplementedException("cascade: cannot resolve the outer column types required by identity (8)");
	}
	vector<LogicalType> outer_types = left->types;

	// The projections between the outer aggregate and the nested one, bottom first.
	vector<LogicalOperator *> between;
	auto current = top.children[0].get();
	while (current != &nested) {
		if (current->type != LogicalOperatorType::LOGICAL_PROJECTION || current->children.size() != 1) {
			throw NotImplementedException("cascade: a correlated scalar subquery whose grouping is not directly "
			                              "below another aggregate is not implemented yet");
		}
		between.push_back(current);
		current = current->children[0].get();
	}

	// (1) The distinct outer values drive the grouping; the original relation is kept
	// for the join that hands the rows back at the end.
	auto dedup_group_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> no_aggregates;
	auto dedup = make_uniq<LogicalAggregate>(dedup_group_index, binder.GenerateTableIndex(),
	                                         std::move(no_aggregates));
	BindingExport dedup_export;
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		dedup->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
		dedup_export.emplace_back(left_bindings[i], ColumnBinding(dedup_group_index, ProjectionIndex(i)));
	}
	dedup->children.push_back(left->Copy(context));

	// (2) The Apply moves below the sub-query's GroupBy, as an inner join so that no
	// padding row can turn into a group.
	auto inner = std::move(nested.children[0]);
	vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
	if (inner->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		ExposeRightColumns(*inner, needed, mapping);
	}
	for (auto &predicate : extracted) {
		RewriteBindings(predicate, mapping);
	}
	if (SubtreeReferencesCorrelation(*inner, correlated)) {
		throw NotImplementedException("cascade: identity (8) does not handle this correlated subquery shape yet "
		                              "(correlated column used below a non row-preserving operator)");
	}
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	join->children.push_back(std::move(dedup));
	join->children.push_back(std::move(inner));
	for (auto &predicate : extracted) {
		AddJoinCondition(*join, std::move(predicate), correlated, false);
	}
	// The predicate is oriented by looking for the correlation domain, so the outer
	// side is repointed at the deduplicated keys only once it has run.
	RewriteOperatorBindings(*join, dedup_export);
	nested.children[0] = std::move(join);

	// (3) The outer keys join the inner grouping, and every projection on the way up
	// has to carry them: a projection does not pass a column it was not asked for.
	vector<ColumnBinding> carry;
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		auto position = nested.groups.size();
		nested.groups.push_back(make_uniq<BoundColumnRefExpression>(
		    outer_types[i], ColumnBinding(dedup_group_index, ProjectionIndex(i))));
		// The grouping sets have to grow with the groups: a set that does not mention a
		// grouping column makes the physical aggregate pad that column with NULL, which
		// silently collapses every row into one group whose key is NULL.
		for (auto &set : nested.grouping_sets) {
			set.insert(ProjectionIndex(position));
		}
		carry.push_back(ColumnBinding(nested.group_index, ProjectionIndex(position)));
	}
	auto thread = [&](const vector<LogicalOperator *> &chain) {
		for (auto *op : chain) {
			auto &projection = op->Cast<LogicalProjection>();
			for (idx_t c = 0; c < carry.size(); c++) {
				auto position = projection.expressions.size();
				projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], carry[c]));
				carry[c] = ColumnBinding(projection.table_index, ProjectionIndex(position));
			}
		}
	};
	thread(between);
	top.groups.clear();
	for (idx_t c = 0; c < carry.size(); c++) {
		top.groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], carry[c]));
	}
	for (auto &set : top.grouping_sets) {
		for (idx_t c = 0; c < carry.size(); c++) {
			set.insert(ProjectionIndex(c));
		}
	}
	carry.clear();
	for (idx_t c = 0; c < left_bindings.size(); c++) {
		carry.push_back(ColumnBinding(top.group_index, ProjectionIndex(c)));
	}
	auto above = projections;
	thread(above);

	// (4) Hand the outer rows their multiplicity back. The sub-query plan above holds
	// one row per distinct outer value, so joining it to the original relation restores
	// the duplicates, and an outer value with no match is padded with NULLs.
	auto expanded = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	expanded->children.push_back(std::move(left));
	expanded->children.push_back(std::move(right));
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		expanded->conditions.emplace_back(
		    make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]),
		    make_uniq<BoundColumnRefExpression>(outer_types[i], carry[i]),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}

	// pi_c: an empty aggregate input is what an unmatched outer value gets from the
	// padding, and count is the aggregate whose answer there is not NULL.
	auto count_at_top = false;
	for (auto &expr : top.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &name = expr->Cast<BoundAggregateExpression>().Function().GetName();
		if (name == "count" || name == "count_star") {
			count_at_top = true;
		}
	}
	if (!count_at_top) {
		return std::move(expanded);
	}
	auto projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> select_list;
	auto outputs = expanded->GetColumnBindings();
	auto types = expanded->types;
	if (types.size() != outputs.size()) {
		expanded->ResolveOperatorTypes();
		types = expanded->types;
	}
	for (idx_t i = 0; i < outputs.size(); i++) {
		if (outputs[i] == value_binding) {
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, types[i]);
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundColumnRefExpression>(types[i], outputs[i]));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
			select_list.push_back(std::move(coalesce));
			exports.emplace_back(value_binding, ColumnBinding(projection_index, ProjectionIndex(i)));
			continue;
		}
		select_list.push_back(make_uniq<BoundColumnRefExpression>(types[i], outputs[i]));
		exports.emplace_back(outputs[i], ColumnBinding(projection_index, ProjectionIndex(i)));
	}
	auto fixup = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
	fixup->children.push_back(std::move(expanded));
	return std::move(fixup);
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateScalar(unique_ptr<LogicalOperator> left,
                                                                unique_ptr<LogicalOperator> right,
                                                                const CorrelatedColumns &correlated,
                                                                BindingExport &exports) {
	// Identity (9) of Galindo-Legaria & Joshi, "Orthogonal Optimization of
	// Subqueries and Aggregation":
	//     R A_x (G_{F1} E)  =  G_{columns(R), F'}( R LOJ E )
	// It holds because SQL aggregates satisfy agg(empty) = agg({null}): a left outer
	// join hands an outer row with no match a single NULL-padded row, and the
	// aggregate over that row is exactly the aggregate over an empty input.
	//
	// Grouping the outer side - rather than grouping the sub-query side and joining
	// one group back per key - is what makes count work. count over the padded row
	// is 1, so F' re-expresses it over a compared sub-query column, which is NULL
	// there.
	vector<LogicalOperator *> projections;
	auto node = right.get();
	while (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		projections.push_back(node);
		node = node->children[0].get();
	}
	if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || node->children.size() != 1) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery is only decorrelated when it aggregates");
	}
	auto &aggregate = node->Cast<LogicalAggregate>();
	if (!aggregate.groups.empty()) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery that already groups is not implemented yet");
	}
	if (aggregate.expressions.size() != 1 ||
	    aggregate.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery over anything but one aggregate is not implemented yet");
	}

	// What the parent reads as the sub-query's value, captured before the sub-tree
	// is replaced by the group-by.
	auto value_binding = right->GetColumnBindings().back();

	vector<unique_ptr<Expression>> extracted;
	node->children[0] = ExtractCorrelatedPredicates(std::move(node->children[0]), correlated, extracted);
	LogicalAggregate *nested = nullptr;
	if (extracted.empty()) {
		// No predicate could be lifted because the correlation sits below a GroupBy of
		// the sub-query's own. That is identity (8)'s case: the Apply has to move below
		// that GroupBy first.
		nested = FindNestedGroupedAggregate(*node->children[0]);
		if (nested) {
			nested->children[0] = ExtractCorrelatedPredicates(std::move(nested->children[0]), correlated, extracted);
		}
	}
	if (extracted.empty()) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery without a correlated predicate is not implemented yet");
	}
	if (nested) {
		return DecorrelateNestedScalar(std::move(left), std::move(right), projections, aggregate, *nested, extracted,
		                               correlated, exports, value_binding);
	}
	if (SubtreeReferencesCorrelation(*node->children[0], correlated)) {
		throw NotImplementedException(
		    "cascade: Apply elimination does not handle this correlated subquery shape yet "
		    "(correlated column used below a non row-preserving operator)");
	}
	// The predicate may take any form, not only equality: identity (9) groups the
	// outer side, so the predicate merely decides which inner rows each group sees.
	// A NULL comparison is never true, so every column the predicate compares is
	// non-NULL on the rows the join keeps - which is what the count rewrite relies
	// on.
	auto needed = CollectRightColumns(extracted, correlated);
	if (needed.empty()) {
		throw NotImplementedException("cascade: a correlated scalar subquery with no sub-query column");
	}

	// Read what F' needs out of the aggregate before the sub-tree is released: the
	// aggregate and the projections above it are replaced by an aggregate below the
	// join, and the projections themselves are re-attached at the end.
	auto value_expression = aggregate.expressions[0]->Copy();
	auto value_type = aggregate.expressions[0]->GetReturnType();
	auto &aggregate_expression = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	auto &name = aggregate_expression.Function().GetName();
	// count is the aggregate whose value on an empty input is not NULL, so it is the
	// one that needs the compensating project pi_c of section 3.2.
	const bool count_rewrite = name == "count" || name == "count_star";

	const auto old_aggregate_index = aggregate.aggregate_index;
	auto inner = std::move(node->children[0]);

	// The lifted predicates are evaluated at the join, which sits above the body's
	// own projections, so every column they name has to be projected out first.
	vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
	if (inner->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		ExposeRightColumns(*inner, needed, mapping);
	}
	for (auto &predicate : extracted) {
		RewriteBindings(predicate, mapping);
	}

	auto left_bindings = left->GetColumnBindings();
	if (left->types.size() != left_bindings.size()) {
		left->ResolveOperatorTypes();
	}
	if (left->types.size() != left_bindings.size()) {
		throw NotImplementedException("cascade: cannot resolve the outer column types required by identity (9)");
	}
	vector<LogicalType> outer_types = left->types;

	// Section 3.2 of the paper moves the GroupBy below the outer join:
	//     G_{A,F}( S LOJ_p R ) = pi_c( S LOJ_p ( G_{A-columns(S),F} R ) )
	// The trade is that an outer row matching nothing no longer hands the aggregate a
	// NULL-padded row: it produces no group at all, and the outerjoin hands the
	// caller NULL, which pi_c repairs wherever NULL is not the empty-input answer
	// (count). That is both cheaper and more faithful than identity (9) - count and
	// list come out right by construction, instead of by the fiction that
	// agg(empty) = agg({null}).
	//
	// The rule needs the predicate's sub-query columns to be functionally determined
	// by the grouping columns, which here are the outer columns themselves. With
	// s.c = <outer expression> the outer row fixes s.c, so at most one group can
	// match and each outer row still contributes exactly one output row - which is
	// also what makes this form correct when the outer relation has duplicate rows,
	// where identity (9) folds them into a single group. A predicate such as
	// s.a + s.b = t.a fixes neither s.a nor s.b, so it stays with identity (9).
	//
	// It also needs the aggregate functions to read only the sub-query's own columns:
	// below the outerjoin the outer columns are not in scope. Identity (9) has no such
	// restriction, since its GroupBy sits above the join - which is how
	// (select sum(t.a) from s where s.a = t.a) is handled.
	vector<ColumnBinding> inner_keys;
	bool pushdown = !ReferencesCorrelation(*aggregate.expressions[0], correlated);
	for (auto &column : needed) {
		auto binding = MapBinding(column.binding, mapping);
		bool determined = false;
		for (auto &predicate : extracted) {
			if (!BoundComparisonExpression::IsComparison(*predicate) ||
			    predicate->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &comparison = predicate->Cast<BoundFunctionExpression>();
			auto &lhs = BoundComparisonExpression::Left(comparison);
			auto &rhs = BoundComparisonExpression::Right(comparison);
			for (idx_t side = 0; side < 2; side++) {
				auto &operand = side == 0 ? lhs : rhs;
				auto &other = side == 0 ? rhs : lhs;
				if (operand.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
				    operand.Cast<BoundColumnRefExpression>().Binding() != binding) {
					continue;
				}
				if (!ReferencesInner(other, correlated)) {
					determined = true;
				}
			}
		}
		if (!determined) {
			pushdown = false;
			break;
		}
		inner_keys.push_back(binding);
	}

	// Both strategies end in the same shape: a node that exposes the outer columns and
	// the sub-query's value, plus a note of where those two have moved to.
	vector<unique_ptr<Expression>> aggregate_list;
	aggregate_list.push_back(std::move(value_expression));
	unique_ptr<LogicalOperator> body;
	vector<ColumnBinding> outer_now;
	ColumnBinding value_now;
	//! Whether the outer rows still have to be handed back their multiplicity.
	bool reexpand = false;

	if (pushdown) {
		auto inner_group_index = binder.GenerateTableIndex();
		auto inner_aggregate_index = binder.GenerateTableIndex();
		auto pushed = make_uniq<LogicalAggregate>(inner_group_index, inner_aggregate_index, std::move(aggregate_list));
		BindingExport key_export;
		for (idx_t i = 0; i < needed.size(); i++) {
			pushed->groups.push_back(make_uniq<BoundColumnRefExpression>(needed[i].type, inner_keys[i]));
			key_export.emplace_back(inner_keys[i], ColumnBinding(inner_group_index, ProjectionIndex(i)));
		}
		pushed->children.push_back(std::move(inner));

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
		join->children.push_back(std::move(left));
		join->children.push_back(std::move(pushed));
		for (auto &predicate : extracted) {
			// The predicate now compares the outer side against the group keys; the
			// sub-query's own columns are no longer exposed by the aggregate below.
			// Plain comparison, not IS NOT DISTINCT FROM: the outer row has to mean
			// exactly what the user wrote, so a NULL outer value matches nothing and
			// receives the empty-input answer from pi_c.
			RewriteBindings(predicate, key_export);
			AddJoinCondition(*join, std::move(predicate), correlated, false);
		}

		// pi_c is only needed where NULL is not the empty-input answer. For every other
		// aggregate the outerjoin already hands the caller exactly what agg(empty)
		// returns - the paper's own example notes that "no computing projects are
		// required here as the aggregate expression sum(...) does result in NULL when
		// calculated on a singleton NULL" - so the join is the whole answer and the
		// outer columns stay where they were on its left side.
		value_now = ColumnBinding(inner_aggregate_index, ProjectionIndex(0));
		if (!count_rewrite) {
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				outer_now.push_back(left_bindings[i]);
			}
			body = std::move(join);
		} else {
			auto projection_index = binder.GenerateTableIndex();
			vector<unique_ptr<Expression>> select_list;
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				select_list.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
				outer_now.push_back(ColumnBinding(projection_index, ProjectionIndex(i)));
			}
			value_now = ColumnBinding(projection_index, ProjectionIndex(select_list.size()));
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, value_type);
			coalesce->GetChildrenMutable().push_back(
			    make_uniq<BoundColumnRefExpression>(value_type, ColumnBinding(inner_aggregate_index, ProjectionIndex(0))));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
			select_list.push_back(std::move(coalesce));
			auto fixup = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
			fixup->children.push_back(std::move(join));
			body = std::move(fixup);
		}
	} else {
		// Identity (9), for the predicates section 3.2 cannot push: group the outer
		// side over a left outer join, so every outer row has a group and the
		// aggregate always sees at least a NULL-padded row.
		//
		// For count that padded row must contribute 0 rather than 1, so the count is
		// re-expressed over a compared sub-query column. Every other SQL aggregate
		// already agrees with the NULL the padded row carries, so it is carried over.
		if (count_rewrite) {
			auto count_binding = MapBinding(needed[0].binding, mapping);
			FunctionBinder function_binder(context);
			vector<LogicalType> argument_types {needed[0].type};
			auto count_function = GetBuiltinAggregateFunction(context, Identifier("count"), argument_types);
			vector<unique_ptr<Expression>> arguments;
			arguments.push_back(make_uniq<BoundColumnRefExpression>(needed[0].type, count_binding));
			aggregate_list[0] = function_binder.BindAggregateFunction(std::move(count_function), std::move(arguments),
			                                                          nullptr, AggregateType::NON_DISTINCT);
		}

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
		// Identity (9) is stated for an outer relation that contains a key: it folds
		// every outer row sharing a grouping value into one group, so duplicate outer
		// rows would lose both their rows and - because that one group then sees the
		// matches of all of them - their value. Instead of leaning on the
		// precondition, the outer side is deduplicated before it is joined and its
		// rows are handed back afterwards, which is what DuckDB's delimited join does
		// with the keys it materialises. Repointing the predicate's outer side at the
		// deduplicated keys needs every predicate to be a comparison; any other shape
		// keeps the plan the paper describes, precondition included.
		bool can_dedup = true;
		for (auto &predicate : extracted) {
			if (!BoundComparisonExpression::IsComparison(*predicate)) {
				can_dedup = false;
			}
		}
		BindingExport outer_export;
		unique_ptr<LogicalOperator> outer_child;
		if (can_dedup) {
			auto dedup_index = binder.GenerateTableIndex();
			vector<unique_ptr<Expression>> no_aggregates;
			auto dedup = make_uniq<LogicalAggregate>(dedup_index, binder.GenerateTableIndex(),
			                                          std::move(no_aggregates));
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				dedup->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
				outer_export.emplace_back(left_bindings[i], ColumnBinding(dedup_index, ProjectionIndex(i)));
			}
			dedup->children.push_back(left->Copy(context));
			outer_child = std::move(dedup);
		} else {
			outer_child = left->Copy(context);
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				outer_export.emplace_back(left_bindings[i], left_bindings[i]);
			}
		}

		join->children.push_back(std::move(outer_child));
		join->children.push_back(std::move(inner));
		for (auto &predicate : extracted) {
			// Plain comparison, not IS NOT DISTINCT FROM. The scalar path has no
			// marker to keep two-valued, and the predicate has to mean exactly what
			// the user wrote: a NULL outer value must match nothing, so that the left
			// outer join supplies the padded row and the aggregate sees an empty
			// input. A NULL-safe comparison would instead let a NULL outer value
			// match a NULL inner row - which is how max/sum over a NULL outer value
			// wrongly returned the inner row's value while count happened to stay
			// right.
			AddJoinCondition(*join, std::move(predicate), correlated, false);
		}
		// AddJoinCondition orients the predicate by looking for the correlation
		// domain, so the keys are substituted only once it has run.
		RewriteOperatorBindings(*join, outer_export);

		auto group_index = binder.GenerateTableIndex();
		auto aggregate_index = binder.GenerateTableIndex();
		auto group_by = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregate_list));
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			group_by->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], outer_export[i].second));
			outer_now.push_back(ColumnBinding(group_index, ProjectionIndex(i)));
		}
		group_by->children.push_back(std::move(join));
		// A sub-query aggregate may read the outer columns itself -
		// (select sum(s.b + t.a) ...) - and those are now the deduplicated keys.
		RewriteOperatorBindings(*group_by, outer_export);
		value_now = ColumnBinding(aggregate_index, ProjectionIndex(0));
		body = std::move(group_by);
		reexpand = can_dedup;
	}

	// The sub-query's own projections are kept rather than discarded: they may
	// compute on top of the aggregate - TPC-H Q20 uses 0.5 * sum(...) - so dropping
	// them would silently change the value the parent reads. They are re-attached
	// above the aggregate, and only the bottom one names the aggregate, so only that
	// reference has to move. Keeping them also means the parent's binding survives
	// untouched, since the topmost projection keeps its table index.
	// Where the outer columns can be read once the shape above is in place: identity
	// (9) has replaced them with its group keys, section 3.2 leaves them where they
	// were on the outerjoin's left side.
	vector<ColumnBinding> pass;
	unique_ptr<LogicalOperator> result;
	if (!projections.empty()) {
		auto &bottom = projections.back()->Cast<LogicalProjection>();
		// Below the aggregate the outer columns and the sub-query value were named by
		// the left child's bindings and the old aggregate index; above it they are the
		// group keys (or the join's columns) and the new aggregate. The bottom
		// projection is the one that straddles that change.
		BindingExport aggregate_export;
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			aggregate_export.emplace_back(left_bindings[i], outer_now[i]);
		}
		aggregate_export.emplace_back(ColumnBinding(old_aggregate_index, ProjectionIndex(0)), value_now);
		for (auto &expr : bottom.expressions) {
			RewriteBindings(expr, aggregate_export);
		}
		bottom.children[0] = std::move(body);

		// The parent of the Apply also reads the outer columns, and the sub-query's
		// own projection does not carry them. Thread them up the chain, appending
		// after the existing expressions so the sub-query value keeps its position -
		// which is what keeps the parent's reference to that value valid.
		pass = outer_now;
		for (idx_t i = projections.size(); i-- > 0;) {
			auto &projection = projections[i]->Cast<LogicalProjection>();
			for (idx_t c = 0; c < pass.size(); c++) {
				auto position = projection.expressions.size();
				projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], pass[c]));
				pass[c] = ColumnBinding(projection.table_index, ProjectionIndex(position));
			}
		}
		result = std::move(right);
	} else {
		pass = outer_now;
		result = std::move(body);
	}

	if (reexpand) {
		// Identity (9) is stated for an outer relation that contains a key, because it
		// folds every outer row sharing a grouping value into one group. Rather than
		// rely on that precondition, hand the outer rows back their multiplicity by
		// joining the aggregated sub-query to them again - which is what DuckDB's
		// delimited join achieves by materialising the distinct keys. The comparison
		// is NULL-safe so an outer row whose correlation column is NULL still finds
		// its own group instead of being dropped by the inner join.
		auto expanded = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		expanded->children.push_back(std::move(left));
		expanded->children.push_back(std::move(result));
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			expanded->conditions.emplace_back(
			    make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]),
			    make_uniq<BoundColumnRefExpression>(outer_types[i], pass[i]),
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		}
		result = std::move(expanded);
		// The outer columns keep the bindings they already had, so only the sub-query
		// value has to be repointed - and only when no sub-query projection carried it.
		if (projections.empty()) {
			exports.emplace_back(value_binding, value_now);
		}
		return result;
	}

	for (idx_t c = 0; c < pass.size(); c++) {
		exports.emplace_back(left_bindings[c], pass[c]);
	}
	if (projections.empty()) {
		exports.emplace_back(value_binding, value_now);
	}
	return result;
}

//! Point an operator's own expressions at the bindings its child now exposes.
static void RewriteOperatorBindings(LogicalOperator &op, const BindingExport &exports) {
	for (auto &expr : op.expressions) {
		RewriteBindings(expr, exports);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			RewriteBindings(condition.LeftReference(), exports);
			RewriteBindings(condition.RightReference(), exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			RewriteBindings(condition, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// An aggregate keeps its grouping expressions in their own member, away from
		// op.expressions, so they need rewriting too.
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			RewriteBindings(group, exports);
		}
		break;
	}
	default:
		break;
	}
}

//! True for operators that expose their child's bindings unchanged. A rewrite
//! below one of them is therefore still visible to its own parent, so the export
//! mapping has to keep travelling upwards.
static bool PassesBindingsThrough(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	default:
		return false;
	}
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateNode(unique_ptr<LogicalOperator> op, BindingExport &exports) {
	for (auto &child : op->children) {
		BindingExport child_exports;
		child = DecorrelateNode(std::move(child), child_exports);
		if (child_exports.empty()) {
			continue;
		}
		RewriteOperatorBindings(*op, child_exports);
		if (PassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		return DecorrelateApply(std::move(op), exports);
	}
	return op;
}

unique_ptr<LogicalOperator> ApplyDecorrelator::Decorrelate(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	BindingExport exports;
	return DecorrelateNode(std::move(plan), exports);
}

//! How often a column binding is read across a plan.
struct BindingUse {
	ColumnBinding binding;
	idx_t count = 0;
};

static void CountUses(const LogicalOperator &op, vector<BindingUse> &uses) {
	auto bump = [&](const ColumnBinding &binding) {
		for (auto &entry : uses) {
			if (entry.binding == binding) {
				entry.count++;
				return;
			}
		}
		uses.push_back(BindingUse {binding, 1});
	};
	auto bump_expression = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) { bump(colref.Binding()); });
	};
	for (auto &expr : op.expressions) {
		bump_expression(*expr);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			bump_expression(condition.GetLHS());
			bump_expression(condition.GetRHS());
		}
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			bump_expression(*condition);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		CountUses(*child, uses);
	}
}

static idx_t UseCount(const vector<BindingUse> &uses, const ColumnBinding &binding) {
	for (auto &entry : uses) {
		if (entry.binding == binding) {
			return entry.count;
		}
	}
	return 0;
}

static unique_ptr<LogicalOperator> SimplifyMarkerJoin(unique_ptr<LogicalOperator> op,
                                                      const vector<BindingUse> &uses) {
	for (auto &child : op->children) {
		child = SimplifyMarkerJoin(std::move(child), uses);
	}
	if (op->type != LogicalOperatorType::LOGICAL_FILTER || op->expressions.size() != 1 || op->children.size() != 1) {
		return op;
	}
	// The predicate is either the marker itself or its negation.
	bool negated = false;
	const Expression *marker = op->expressions[0].get();
	if (marker->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    marker->GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		auto &not_expr = marker->Cast<BoundOperatorExpression>();
		if (not_expr.GetChildren().size() != 1) {
			return op;
		}
		marker = not_expr.GetChildren()[0].get();
		negated = true;
	}
	if (marker->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return op;
	}
	auto &child = *op->children[0];
	if (child.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	auto &join = child.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK) {
		return op;
	}
	auto marker_binding = ColumnBinding(join.mark_index, ProjectionIndex(0));
	if (marker->Cast<BoundColumnRefExpression>().Binding() != marker_binding) {
		return op;
	}
	// Anything else reading the marker needs it to keep existing.
	if (UseCount(uses, marker_binding) != 1) {
		return op;
	}
	// `NOT mark` is anti-join shaped only while the marker is two-valued, which
	// holds exactly when every comparison is NULL-safe: then the marker is never
	// unknown and "no match" coincides with "not true". NOT IN over a list that
	// contains a NULL keeps a three-valued marker and has to stay a MARK join.
	if (negated) {
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			auto type = condition.GetComparisonType();
			if (type != ExpressionType::COMPARE_DISTINCT_FROM &&
			    type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				return op;
			}
		}
	}
	// The NULL-safety and the right-side NULL stripping exist only to keep the
	// marker two-valued. A semi/anti join evaluates its condition directly, so
	// plain equality carries the same meaning there, and the stripping filter this
	// join carries becomes redundant. DuckDB's SimplifyNullSafeSemiJoinConditions
	// makes the same trade.
	bool drop_null_filter = false;
	if (join.children[1]->type == LogicalOperatorType::LOGICAL_FILTER) {
		drop_null_filter = true;
		for (auto &expr : join.children[1]->expressions) {
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
			    expr->GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
				drop_null_filter = false;
				break;
			}
		}
	}
	for (auto &condition : join.conditions) {
		if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			condition = JoinCondition(condition.LeftReference()->Copy(), condition.RightReference()->Copy(),
			                          ExpressionType::COMPARE_EQUAL);
		}
	}
	if (drop_null_filter) {
		join.children[1] = std::move(join.children[1]->children[0]);
	}
	join.join_type = negated ? JoinType::ANTI : JoinType::SEMI;
	return std::move(op->children[0]);
}

unique_ptr<LogicalOperator> SimplifyMarkerJoins(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	vector<BindingUse> uses;
	CountUses(*plan, uses);
	return SimplifyMarkerJoin(std::move(plan), uses);
}

} // namespace duckdb
