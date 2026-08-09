#include "duckdb/optimizer/aggregate_rewrite_helper.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

vector<Identifier> AggregateRewriteHelper::GenerateColumnNames(const string &prefix, idx_t column_count) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		result.emplace_back(StringUtil::Format("%s_%llu", prefix, col_idx));
	}
	return result;
}

static void RebindExpression(unique_ptr<Expression> &expr, const column_binding_map_t<ColumnBinding> &replacement_map) {
	if (!expr || replacement_map.empty()) {
		return;
	}
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    auto entry = replacement_map.find(colref.Binding());
		    if (entry != replacement_map.end()) {
			    colref.BindingMutable() = entry->second;
		    }
	    });
}

unique_ptr<Expression>
AggregateRewriteHelper::CopyAndRebind(const Expression &expr,
                                      const column_binding_map_t<ColumnBinding> &replacement_map) {
	auto result = expr.Copy();
	RebindExpression(result, replacement_map);
	return result;
}

static void StageVolatileExpression(unique_ptr<Expression> &expr, TableIndex projection_index,
                                    vector<unique_ptr<Expression>> &projection_expressions) {
	if (!expr || !expr->IsVolatile()) {
		return;
	}
	auto return_type = expr->GetReturnType();
	const auto column_index = projection_expressions.size();
	projection_expressions.push_back(std::move(expr));
	expr = make_uniq<BoundColumnRefExpression>(return_type,
	                                           ColumnBinding(projection_index, ProjectionIndex(column_index)));
}

void AggregateRewriteHelper::StageVolatileAggregateInputs(Optimizer &optimizer, LogicalAggregate &aggr,
                                                          unique_ptr<LogicalOperator> &child) {
	// Branching rewrites must not multiply the evaluation of volatile aggregate inputs.
	bool has_volatile_input = false;
	for (auto &group : aggr.groups) {
		has_volatile_input |= group->IsVolatile();
	}
	for (auto &expr : aggr.expressions) {
		has_volatile_input |= expr->IsVolatile();
	}
	if (!has_volatile_input) {
		return;
	}

	child->ResolveOperatorTypes();
	auto child_bindings = child->GetColumnBindings();
	auto projection_index = optimizer.binder.GenerateTableIndex();

	column_binding_map_t<ColumnBinding> projection_replacements;
	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.reserve(child_bindings.size());
	for (idx_t col_idx = 0; col_idx < child_bindings.size(); col_idx++) {
		projection_replacements[child_bindings[col_idx]] = ColumnBinding(projection_index, ProjectionIndex(col_idx));
		projection_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(child->types[col_idx], child_bindings[col_idx]));
	}

	for (auto &group : aggr.groups) {
		StageVolatileExpression(group, projection_index, projection_expressions);
	}
	for (auto &expr : aggr.expressions) {
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggregate.GetChildrenMutable()) {
			StageVolatileExpression(child_expr, projection_index, projection_expressions);
		}
		if (aggregate.GetOrderBys()) {
			for (auto &order : aggregate.GetOrderBysMutable()->orders) {
				StageVolatileExpression(order.expression, projection_index, projection_expressions);
			}
		}
		StageVolatileExpression(aggregate.GetFilterMutable(), projection_index, projection_expressions);
	}

	for (auto &group : aggr.groups) {
		RebindExpression(group, projection_replacements);
	}
	for (auto &expr : aggr.expressions) {
		RebindExpression(expr, projection_replacements);
	}

	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_expressions));
	projection->children.push_back(std::move(child));
	child = std::move(projection);
}

unique_ptr<LogicalOperator> AggregateRewriteHelper::CreateCTERef(Optimizer &optimizer, TableIndex cte_index,
                                                                 const vector<LogicalType> &input_types,
                                                                 const vector<Identifier> &input_names,
                                                                 const vector<ColumnBinding> &input_bindings,
                                                                 column_binding_map_t<ColumnBinding> &replacement_map) {
	auto cte_ref_index = optimizer.binder.GenerateTableIndex();
	for (idx_t col_idx = 0; col_idx < input_bindings.size(); col_idx++) {
		replacement_map[input_bindings[col_idx]] = ColumnBinding(cte_ref_index, ProjectionIndex(col_idx));
	}
	return make_uniq<LogicalCTERef>(cte_ref_index, cte_index, input_types, input_names);
}

} // namespace duckdb
