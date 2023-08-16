#include "duckdb/optimizer/cascade/NewColumnBindingResolver.h"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/operator/schema/physical_create_index.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/to_string.hpp"

namespace gpopt
{
NewColumnBindingResolver::NewColumnBindingResolver()
{
}

void NewColumnBindingResolver::VisitOperatorChildren(PhysicalOperator &op)
{
	for (auto &child : op.children)
	{
		PhysicalOperator* input = (PhysicalOperator*)child.get();
		VisitOperator(*input);
	}
}

void NewColumnBindingResolver::VisitOperator(PhysicalOperator &op)
{
	switch (op.physical_type)
	{
    case PhysicalOperatorType::NESTED_LOOP_JOIN:
    case PhysicalOperatorType::DELIM_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	{
		// special case: comparison join
		auto &comp_join = op.Cast<PhysicalComparisonJoin>();
		// first get the bindings of the LHS and resolve the LHS expressions
		PhysicalOperator* left_op = (PhysicalOperator*)comp_join.children[0].get();
		VisitOperator(*left_op);
		for (auto &cond : comp_join.conditions)
		{
			VisitExpression(&cond.left);
		}
		if (left_op->physical_type == PhysicalOperatorType::DELIM_JOIN)
		{
			// visit the duplicate eliminated columns on the LHS, if any
			auto &delim_join = op.Cast<PhysicalDelimJoin>();
			for (auto scan : delim_join.delim_scans)
			{
			}
		}
		// then get the bindings of the RHS and resolve the RHS expressions
		PhysicalOperator* right_op = (PhysicalOperator*)comp_join.children[1].get();
		VisitOperator(*right_op);
		for (auto &cond : comp_join.conditions)
		{
			VisitExpression(&cond.right);
		}
		// finally update the bindings with the result bindings of the join
		bindings = op.GetColumnBindings();
		return;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		//! We first need to update the current set of bindings and then visit operator expressions
		bindings = op.GetColumnBindings();
		VisitOperatorExpressions(op);
		return;
	}
	case PhysicalOperatorType::PROJECTION: {
		VisitOperatorChildren(op);
		// special case: projection
		auto &proj = op.Cast<PhysicalProjection>();
		/* Notice: we do not update bindings here because only the binding from children is correct */
		/* the GetColumnBindings in Projection is for subquery (I guess) */
		for (auto &cond : proj.select_list)
		{
			VisitExpression(&cond);
		}
		bindings = op.GetColumnBindings();
		return;
	}
	default:
		break;
	}
	// general case
	// first visit the children of this operator
	VisitOperatorChildren(op);
	// now visit the expressions of this operator to resolve any bound column references
	VisitOperatorExpressions(op);
	// finally update the current set of bindings to the current set of column bindings
	bindings = op.GetColumnBindings();
}

void
NewColumnBindingResolver::EnumerateExpressions(PhysicalOperator &op,
                                            const std::function<void(duckdb::unique_ptr<Expression> *child)> &callback)
{
	switch (op.physical_type)
	{
	case PhysicalOperatorType::EXPRESSION_SCAN:
	{
		auto &get = op.Cast<PhysicalExpressionScan>();
		for (auto &expr_list : get.expressions)
		{
			for (auto &expr : expr_list)
			{
				callback(&expr);
			}
		}
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
		auto &order = op.Cast<PhysicalOrder>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case PhysicalOperatorType::TOP_N: {
		auto &order = op.Cast<PhysicalTopN>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::DELIM_JOIN:
	case PhysicalOperatorType::HASH_JOIN: {
		if (op.physical_type == PhysicalOperatorType::DELIM_JOIN) {
			auto &delim_join = op.Cast<PhysicalDelimJoin>();
			for (auto &expr : delim_join.delim_scans) {
			}
		}
		auto &join = op.Cast<PhysicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			callback(&cond.left);
			callback(&cond.right);
		}
		break;
	}
	case PhysicalOperatorType::LIMIT: {
		auto &limit = op.Cast<PhysicalLimit>();
		if (limit.limit_expression) {
			callback(&limit.limit_expression);
		}
		if (limit.offset_expression) {
			callback(&limit.offset_expression);
		}
		break;
	}
	case PhysicalOperatorType::LIMIT_PERCENT: {
		auto &limit = (PhysicalLimitPercent &)op;
		if (limit.limit_expression) {
			callback(&limit.limit_expression);
		}
		if (limit.offset_expression) {
			callback(&limit.offset_expression);
		}
		break;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggr = op.Cast<PhysicalUngroupedAggregate>();
		for (auto &group : aggr.aggregates) {
			callback(&group);
		}
		break;
	}
	default:
		break;
	}
	for (auto &expression : op.expressions) {
		callback(&expression);
	}
}

void NewColumnBindingResolver::VisitOperatorExpressions(PhysicalOperator &op) {
	NewColumnBindingResolver::EnumerateExpressions(op, [&](duckdb::unique_ptr<Expression> *child) { VisitExpression(child); });
}

void NewColumnBindingResolver::VisitExpression(duckdb::unique_ptr<Expression> *expression) {
	auto &expr = **expression;
	duckdb::unique_ptr<Expression> result;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		result = VisitReplace(expr.Cast<BoundAggregateExpression>(), expression);
		break;
	case ExpressionClass::BOUND_BETWEEN:
		result = VisitReplace(expr.Cast<BoundBetweenExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CASE:
		result = VisitReplace(expr.Cast<BoundCaseExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CAST:
		result = VisitReplace(expr.Cast<BoundCastExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = VisitReplace(expr.Cast<BoundColumnRefExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = VisitReplace(expr.Cast<BoundComparisonExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = VisitReplace(expr.Cast<BoundConjunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = VisitReplace(expr.Cast<BoundConstantExpression>(), expression);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = VisitReplace(expr.Cast<BoundFunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		result = VisitReplace(expr.Cast<BoundSubqueryExpression>(), expression);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = VisitReplace(expr.Cast<BoundOperatorExpression>(), expression);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = VisitReplace(expr.Cast<BoundParameterExpression>(), expression);
		break;
	case ExpressionClass::BOUND_REF:
		result = VisitReplace(expr.Cast<BoundReferenceExpression>(), expression);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = VisitReplace(expr.Cast<BoundDefaultExpression>(), expression);
		break;
	case ExpressionClass::BOUND_WINDOW:
		result = VisitReplace(expr.Cast<BoundWindowExpression>(), expression);
		break;
	case ExpressionClass::BOUND_UNNEST:
		result = VisitReplace(expr.Cast<BoundUnnestExpression>(), expression);
		break;
	default:
		throw InternalException("Unrecognized expression type in logical operator visitor");
	}
	if (result) {
		*expression = std::move(result);
	} else {
		// visit the children of this node
		VisitExpressionChildren(expr);
	}
}

void NewColumnBindingResolver::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](duckdb::unique_ptr<Expression> &expr) { VisitExpression(&expr); });
}

duckdb::unique_ptr<Expression>
NewColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                    duckdb::unique_ptr<Expression> *expr_ptr)
{
	D_ASSERT(expr.depth == 0);
	// check the current set of column bindings to see which index corresponds to the column reference
	for (idx_t i = 0; i < bindings.size(); i++)
	{
		if (expr.binding == bindings[i])
		{
			return make_uniq<BoundReferenceExpression>(expr.alias, expr.return_type, i);
		}
	}
	// LCOV_EXCL_START
	// could not bind the column reference, this should never happen and indicates a bug in the code
	// generate an error message
	string bound_columns = "[";
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i != 0) {
			bound_columns += " ";
		}
		bound_columns += to_string(bindings[i].table_index) + "." + to_string(bindings[i].column_index);
	}
	bound_columns += "]";
	throw InternalException("Failed to bind column reference \"%s\" [%d.%d] (bindings: %s)", expr.alias, expr.binding.table_index, expr.binding.column_index, bound_columns);
	// LCOV_EXCL_STOP
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundAggregateExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundBetweenExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundCaseExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundCastExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundComparisonExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundConjunctionExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundConstantExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundDefaultExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundFunctionExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundOperatorExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundParameterExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundReferenceExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundSubqueryExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundWindowExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

duckdb::unique_ptr<Expression> NewColumnBindingResolver::VisitReplace(BoundUnnestExpression &expr,
                                                            duckdb::unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

} // namespace duckdb
