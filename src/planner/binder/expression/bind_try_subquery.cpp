#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

#include <algorithm>

namespace duckdb {

static unique_ptr<Expression> WrapInTry(unique_ptr<Expression> child) {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_TRY, child->GetReturnType());
	result->GetChildrenMutable().push_back(std::move(child));
	return std::move(result);
}

//! Resolve the referenced columns against the operators below the projection. Columns computed by an
//! aggregate are collected for gating, columns computed by a window or unnest are not maskable positions.
static void FindColumnSources(LogicalOperator &op, const vector<ColumnBinding> &refs, const OperatorExpression &try_op,
                              optional_ptr<LogicalAggregate> &aggregate, vector<idx_t> &aggregate_positions) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = op.Cast<LogicalWindow>();
		for (auto &ref : refs) {
			if (ref.table_index == window.window_index) {
				throw BinderException(try_op,
				                      "TRY over a subquery with a window function in its select list is not supported");
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = op.Cast<LogicalUnnest>();
		for (auto &ref : refs) {
			if (ref.table_index == unnest.unnest_index) {
				throw BinderException(try_op, "TRY over a subquery with UNNEST in its select list is not supported");
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		for (auto &ref : refs) {
			if (ref.table_index == aggr.aggregate_index) {
				D_ASSERT(!aggregate || aggregate.get() == &aggr);
				aggregate = &aggr;
				aggregate_positions.push_back(ref.column_index);
			}
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		FindColumnSources(*child, refs, try_op, aggregate, aggregate_positions);
	}
}

void ExpressionBinder::GateTryAggregates(const OperatorExpression &op, LogicalProjection &projection,
                                         idx_t projection_idx, LogicalAggregate &aggregate,
                                         const vector<idx_t> &aggregate_positions) {
	// wrap the aggregate inputs in TRY and collect HAS_ERROR probes over the same inputs
	unique_ptr<Expression> gate;
	for (auto position : aggregate_positions) {
		auto &aggr_expr = aggregate.expressions[position]->Cast<BoundAggregateExpression>();
		if (aggr_expr.GetFilter()) {
			throw BinderException(op, "TRY over a subquery with an aggregate FILTER clause is not supported");
		}
		if (aggr_expr.GetOrderBys()) {
			throw BinderException(op, "TRY over a subquery with an ordered aggregate is not supported");
		}
		if (aggr_expr.StateExportMode() != AggregateStateExportMode::NONE) {
			throw BinderException(op, "TRY over a subquery with an exported aggregate state is not supported");
		}
		for (auto &child : aggr_expr.GetChildrenMutable()) {
			if (child->IsVolatile()) {
				throw BinderException(op, "TRY can not be used in combination with a volatile function");
			}
			auto probe = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_HAS_ERROR, LogicalType::BOOLEAN);
			probe->GetChildrenMutable().push_back(child->Copy());
			if (gate) {
				gate = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(gate),
				                                             std::move(probe));
			} else {
				gate = std::move(probe);
			}
			child = WrapInTry(std::move(child));
		}
	}
	if (!gate) {
		// aggregates without inputs, such as count(*), have nothing to mask
		return;
	}
	// aggregate the probes, so that an error in any input row raises the gate
	auto &system_catalog = Catalog::GetSystemCatalog(context);
	auto &bool_or = system_catalog.GetEntry<AggregateFunctionCatalogEntry>(
	    context, QualifiedName(system_catalog.GetName(), DEFAULT_SCHEMA, "bool_or"));
	vector<pair<Identifier, unique_ptr<Expression>>> gate_children;
	gate_children.emplace_back(Identifier(), std::move(gate));
	ErrorData error;
	FunctionBinder function_binder(context);
	auto gate_aggregate = function_binder.BindAggregateFunction(bool_or, std::move(gate_children), error);
	if (!gate_aggregate) {
		error.AddQueryLocation(op);
		error.Throw();
	}
	ColumnBinding gate_binding(aggregate.aggregate_index, ProjectionIndex(aggregate.expressions.size()));
	aggregate.expressions.push_back(std::move(gate_aggregate));

	// gate the projected expression: an input error turns the aggregated result into NULL
	auto &expr = projection.expressions[projection_idx];
	auto case_expr = make_uniq<BoundCaseExpression>(expr->GetReturnType());
	BoundCaseCheck check;
	check.when_expr = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, gate_binding);
	check.then_expr = make_uniq<BoundConstantExpression>(Value(expr->GetReturnType()));
	case_expr->CaseChecksMutable().push_back(std::move(check));
	case_expr->ElseMutable() = std::move(expr);
	expr = std::move(case_expr);
}

void ExpressionBinder::LowerTrySubquery(const OperatorExpression &op, LogicalOperator &root) {
	// walk from the plan root to the projection computing the single output column of the subquery
	auto binding = root.GetColumnBindings()[0];
	reference<LogicalOperator> current(root);
	optional_ptr<LogicalProjection> forward_projection;
	idx_t forward_idx = 0;
	while (true) {
		auto &current_op = current.get();
		switch (current_op.type) {
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_TOP_N:
		case LogicalOperatorType::LOGICAL_DISTINCT:
		case LogicalOperatorType::LOGICAL_SAMPLE:
		case LogicalOperatorType::LOGICAL_FILTER:
			current = *current_op.children[0];
			continue;
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			current = *current_op.children[1];
			continue;
		case LogicalOperatorType::LOGICAL_WINDOW:
			if (binding.table_index == current_op.Cast<LogicalWindow>().window_index) {
				throw BinderException(op,
				                      "TRY over a subquery with a window function in its select list is not supported");
			}
			current = *current_op.children[0];
			continue;
		case LogicalOperatorType::LOGICAL_UNNEST:
			if (binding.table_index == current_op.Cast<LogicalUnnest>().unnest_index) {
				throw BinderException(op, "TRY over a subquery with UNNEST in its select list is not supported");
			}
			current = *current_op.children[0];
			continue;
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &projection = current_op.Cast<LogicalProjection>();
			if (binding.table_index != projection.table_index) {
				// a column forwarded from outside this projection, nothing to lower
				return;
			}
			auto &expr = projection.expressions[binding.column_index];
			if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    expr->Cast<BoundColumnRefExpression>().Depth() == 0) {
				// a forwarding projection, such as the prune projection: follow the column down
				forward_projection = &projection;
				forward_idx = binding.column_index;
				binding = expr->Cast<BoundColumnRefExpression>().Binding();
				current = *projection.children[0];
				continue;
			}
			// the projection computing the select item: classify its inputs
			vector<ColumnBinding> refs;
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*expr,
			                                                              [&](const BoundColumnRefExpression &colref) {
				                                                              if (colref.Depth() == 0) {
					                                                              refs.push_back(colref.Binding());
				                                                              }
			                                                              });
			optional_ptr<LogicalAggregate> aggregate;
			vector<idx_t> aggregate_positions;
			if (!refs.empty() && !projection.children.empty()) {
				FindColumnSources(*projection.children[0], refs, op, aggregate, aggregate_positions);
			}
			if (aggregate) {
				// the select item aggregates: gate it on HAS_ERROR probes of the aggregate inputs
				std::sort(aggregate_positions.begin(), aggregate_positions.end());
				aggregate_positions.erase(std::unique(aggregate_positions.begin(), aggregate_positions.end()),
				                          aggregate_positions.end());
				GateTryAggregates(op, projection, binding.column_index, *aggregate, aggregate_positions);
				return;
			}
			// a plain projection: push TRY onto the projected expression
			if (expr->IsVolatile()) {
				throw BinderException(op, "TRY can not be used in combination with a volatile function");
			}
			expr = WrapInTry(std::move(expr));
			return;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = current_op.Cast<LogicalAggregate>();
			if (binding.table_index == aggregate.aggregate_index) {
				if (!forward_projection) {
					throw InternalException("TRY subquery: aggregate result without a projection above it");
				}
				vector<idx_t> aggregate_positions {binding.column_index};
				GateTryAggregates(op, *forward_projection, forward_idx, aggregate, aggregate_positions);
			}
			// a bare group or GROUPING() reference cannot raise an error in the projection
			return;
		}
		default:
			// a bare column computed below the select list (scan, join, set operation):
			// TRY has nothing to mask, errors below the projection propagate
			return;
		}
	}
}

//! TRY over a scalar subquery binds the subquery as an ordinary subquery and lowers the TRY onto the bound
//! plan before the subquery is flattened, so names, types, overloads and aggregate ownership resolve exactly
//! as without TRY. In the plain projection case, TRY moves onto the projected expression, which yields a NULL
//! result under the one-row contract of a scalar subquery. In the aggregate case, the projected expression is
//! gated on HAS_ERROR probes of the aggregate inputs, so that an input error anywhere turns the aggregated
//! result into NULL instead of a partially aggregated value. Errors raised outside these positions, such as
//! in the WHERE clause, in nested subqueries or by the one-row cardinality check, propagate deliberately.
BindResult ExpressionBinder::BindTrySubquery(OperatorExpression &op, idx_t depth) {
	D_ASSERT(op.GetChildrenMutable().size() == 1);
	auto &subquery_expr = op.GetChildrenMutable()[0]->Cast<SubqueryExpression>();
	D_ASSERT(subquery_expr.GetSubqueryType() == SubqueryType::SCALAR);
	auto &node = *subquery_expr.SubqueryMutable()->node;
	if (node.type != QueryNodeType::SELECT_NODE) {
		throw BinderException(op, "TRY over a subquery is only supported for plain SELECT subqueries");
	}
	auto &select_node = node.Cast<SelectNode>();
	if (select_node.select_list.size() == 1) {
		// TRY over a select item that is itself a scalar subquery lowers inside that subquery
		auto &select_item = select_node.select_list[0];
		if (select_item->GetExpressionClass() == ExpressionClass::SUBQUERY &&
		    select_item->Cast<SubqueryExpression>().GetSubqueryType() == SubqueryType::SCALAR) {
			auto alias = select_item->GetAlias();
			auto try_item = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
			try_item->SetQueryLocation(select_item->GetQueryLocation());
			try_item->GetChildrenMutable().push_back(std::move(select_item));
			if (!alias.empty()) {
				try_item->SetAlias(alias);
			}
			select_item = std::move(try_item);
		}
	}

	// bind as an ordinary scalar subquery, so inner scopes, overload resolution and aggregate
	// ownership resolve exactly as without TRY
	const auto was_inside_try = inside_try;
	inside_try = false;
	auto result = BindExpression(subquery_expr, depth);
	inside_try = was_inside_try;
	if (result.HasError()) {
		return result;
	}
	auto &bound_subquery = result.expression->Cast<BoundSubqueryExpression>();
	if (bound_subquery.SubqueryMutable().plan) {
		LowerTrySubquery(op, *bound_subquery.SubqueryMutable().plan);
	}
	return result;
}

} // namespace duckdb
