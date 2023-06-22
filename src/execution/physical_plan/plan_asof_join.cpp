#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/join/physical_asof_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_asof_join.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAsOfJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	D_ASSERT(left && right);

	//	Validate
	vector<idx_t> equi_indexes;
	auto asof_idx = op.conditions.size();
	for (size_t c = 0; c < op.conditions.size(); ++c) {
		auto &cond = op.conditions[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			equi_indexes.emplace_back(c);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			D_ASSERT(asof_idx == op.conditions.size());
			asof_idx = c;
			break;
		default:
			throw InternalException("Invalid ASOF JOIN comparison");
		}
	}
	D_ASSERT(asof_idx < op.conditions.size());

	if (!ClientConfig::GetConfig(context).force_asof_iejoin) {
		return make_uniq<PhysicalAsOfJoin>(op, std::move(left), std::move(right));
	}

	//	Strip extra column from rhs projections
	auto &right_projection_map = op.right_projection_map;
	if (right_projection_map.empty()) {
		const auto right_count = right->types.size();
		right_projection_map.reserve(right_count);
		for (column_t i = 0; i < right_count; ++i) {
			right_projection_map.emplace_back(i);
		}
	}

	//	Debug implementation: IEJoin of Window
	//	LEAD(asof_column, 1, infinity) OVER (PARTITION BY equi_column... ORDER BY asof_column) AS asof_temp
	auto &asof_comp = op.conditions[asof_idx];
	auto &asof_column = asof_comp.right;
	auto asof_type = asof_column->return_type;
	auto asof_temp = make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_LEAD, asof_type, nullptr, nullptr);
	asof_temp->children.emplace_back(asof_column->Copy());
	asof_temp->offset_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
	// TODO: If infinities are not supported for a type, fake them by looking at LHS statistics?
	asof_temp->default_expr = make_uniq<BoundConstantExpression>(Value::Infinity(asof_type));
	for (auto equi_idx : equi_indexes) {
		asof_temp->partitions.emplace_back(op.conditions[equi_idx].right->Copy());
	}
	asof_temp->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, asof_column->Copy());
	asof_temp->start = WindowBoundary::UNBOUNDED_PRECEDING;
	asof_temp->end = WindowBoundary::CURRENT_ROW_ROWS;

	vector<unique_ptr<Expression>> window_select;
	window_select.emplace_back(std::move(asof_temp));

	auto &window_types = op.children[1]->types;
	window_types.emplace_back(asof_type);

	auto window = make_uniq<PhysicalWindow>(window_types, std::move(window_select), rhs_cardinality);
	window->children.emplace_back(std::move(right));

	// IEJoin(left, window, conditions || asof_column < asof_temp)
	JoinCondition asof_upper;
	asof_upper.left = asof_comp.left->Copy();
	asof_upper.right = make_uniq<BoundReferenceExpression>(asof_type, window_types.size() - 1);
	asof_upper.comparison = ExpressionType::COMPARE_LESSTHAN;
	op.conditions.emplace_back(std::move(asof_upper));

	return make_uniq<PhysicalIEJoin>(op, std::move(left), std::move(window), std::move(op.conditions), op.join_type,
	                                 lhs_cardinality);
}

} // namespace duckdb
