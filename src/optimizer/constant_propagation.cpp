#include "duckdb/optimizer/constant_propagation.hpp"

#include "duckdb/optimizer/rule/constant_folding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

ConstantPropagation::ConstantPropagation(ClientContext &context) : context(context) {
}

bool ConstantPropagation::RewriteExpression(unique_ptr<Expression> &expr, const ConstantMap &constants) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		auto entry = constants.find(colref.Binding());
		if (colref.Depth() != 0 || entry == constants.end()) {
			return false;
		}
		auto replacement = make_uniq<BoundConstantExpression>(entry->second);
		replacement->SetAlias(expr->GetAlias());
		expr = std::move(replacement);
		return true;
	}
	bool changed = false;
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { changed |= RewriteExpression(child, constants); });
	if (changed && !ExpressionBarrier::Contains(*expr)) {
		auto replacement = ConstantFoldingRule::TryFold(context, *expr);
		if (replacement) {
			replacement->SetAlias(expr->GetAlias());
			expr = std::move(replacement);
		}
	}
	return changed;
}

static bool CanPropagateConstants(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		// Null extension and the cardinality checks of other join types need their own proofs.
		return op.Cast<LogicalJoin>().join_type == JoinType::INNER;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return true;
	default:
		return false;
	}
}

void ConstantPropagation::RewriteExpressions(LogicalOperator &op, const ConstantMap &constants) {
	if (constants.empty()) {
		return;
	}

	const bool preserve_effects = op.type == LogicalOperatorType::LOGICAL_FILTER ||
	                              op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	                              op.type == LogicalOperatorType::LOGICAL_ANY_JOIN;
	if (preserve_effects) {
		vector<unique_ptr<Expression>> rewritten_expressions;
		bool has_effects = false;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			auto rewritten = (*expr)->Copy();
			RewriteExpression(rewritten, constants);
			has_effects |= ExpressionBarrier::Required(*rewritten);
			rewritten_expressions.push_back(std::move(rewritten));
		});
		// Keep bindings and predicate order if any expression still has an observable effect.
		if (!has_effects) {
			idx_t expression_index = 0;
			LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
				*expr = std::move(rewritten_expressions[expression_index++]);
			});
		}
		return;
	}
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expr) { RewriteExpression(*expr, constants); });
}

ConstantPropagation::ConstantMap
ConstantPropagation::ExtractConstants(TableIndex table_index, const vector<unique_ptr<Expression>> &expressions) {
	ConstantMap result;
	for (idx_t i = 0; i < expressions.size(); i++) {
		auto &expr = *expressions[i];
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			result.emplace(ColumnBinding(table_index, ProjectionIndex(i)),
			               expr.Cast<BoundConstantExpression>().GetValue());
		}
	}
	return result;
}

ConstantPropagation::ConstantMap ConstantPropagation::GetOutputConstants(LogicalOperator &op,
                                                                         const ConstantMap &constants) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		return ExtractConstants(projection.table_index, projection.expressions);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &get = op.Cast<LogicalExpressionGet>();
		if (get.expressions.size() != 1) {
			return {};
		}
		return ExtractConstants(get.table_index, get.expressions[0]);
	}
	default:
		break;
	}

	ConstantMap result;
	for (auto &binding : op.GetColumnBindings()) {
		auto entry = constants.find(binding);
		if (entry != constants.end()) {
			result.emplace(binding, entry->second);
		}
	}
	return result;
}

ConstantPropagation::ConstantMap ConstantPropagation::Propagate(LogicalOperator &op) {
	ConstantMap constants;
	for (auto &child : op.children) {
		auto child_constants = Propagate(*child);
		constants.insert(child_constants.begin(), child_constants.end());
	}
	if (!CanPropagateConstants(op)) {
		return {};
	}

	RewriteExpressions(op, constants);
	return GetOutputConstants(op, constants);
}

void ConstantPropagation::Optimize(LogicalOperator &op) {
	Propagate(op);
}

} // namespace duckdb
