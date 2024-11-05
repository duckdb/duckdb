#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/rule/ordered_aggregate_optimizer.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct &op) {
	D_ASSERT(op.children.size() == 1);
	auto child = CreatePlan(*op.children[0]);
	auto &distinct_targets = op.distinct_targets;
	D_ASSERT(child);
	D_ASSERT(!distinct_targets.empty());

	auto &types = child->GetTypes();
	vector<unique_ptr<Expression>> groups, aggregates, projections;
	idx_t group_count = distinct_targets.size();
	unordered_map<idx_t, idx_t> group_by_references;
	vector<LogicalType> aggregate_types;
	// creates one group per distinct_target
	for (idx_t i = 0; i < distinct_targets.size(); i++) {
		auto &target = distinct_targets[i];
		if (target->type == ExpressionType::BOUND_REF) {
			auto &bound_ref = target->Cast<BoundReferenceExpression>();
			group_by_references[bound_ref.index] = i;
		}
		aggregate_types.push_back(target->return_type);
		groups.push_back(std::move(target));
	}
	bool requires_projection = false;
	if (types.size() != group_count) {
		requires_projection = true;
	}
	// we need to create one aggregate per column in the select_list
	for (idx_t i = 0; i < types.size(); ++i) {
		auto logical_type = types[i];
		// check if we can directly refer to a group, or if we need to push an aggregate with FIRST
		auto entry = group_by_references.find(i);
		if (entry != group_by_references.end()) {
			auto group_index = entry->second;
			// entry is found: can directly refer to a group
			projections.push_back(make_uniq<BoundReferenceExpression>(logical_type, group_index));
			if (group_index != i) {
				// we require a projection only if this group element is out of order
				requires_projection = true;
			}
		} else {
			if (op.distinct_type == DistinctType::DISTINCT && op.order_by) {
				throw InternalException("Entry that is not a group, but not a DISTINCT ON aggregate");
			}
			// entry is not one of the groups: need to push a FIRST aggregate
			auto bound = make_uniq<BoundReferenceExpression>(logical_type, i);
			vector<unique_ptr<Expression>> first_children;
			first_children.push_back(std::move(bound));

			FunctionBinder function_binder(context);
			auto first_aggregate =
			    function_binder.BindAggregateFunction(FirstFunctionGetter::GetFunction(logical_type),
			                                          std::move(first_children), nullptr, AggregateType::NON_DISTINCT);
			first_aggregate->order_bys = op.order_by ? op.order_by->Copy() : nullptr;

			if (ClientConfig::GetConfig(context).enable_optimizer) {
				bool changes_made = false;
				auto new_expr = OrderedAggregateOptimizer::Apply(context, *first_aggregate, groups, changes_made);
				if (new_expr) {
					D_ASSERT(new_expr->return_type == first_aggregate->return_type);
					D_ASSERT(new_expr->type == ExpressionType::BOUND_AGGREGATE);
					first_aggregate = unique_ptr_cast<Expression, BoundAggregateExpression>(std::move(new_expr));
				}
			}
			// add the projection
			projections.push_back(make_uniq<BoundReferenceExpression>(logical_type, group_count + aggregates.size()));
			// push it to the list of aggregates
			aggregate_types.push_back(logical_type);
			aggregates.push_back(std::move(first_aggregate));
			requires_projection = true;
		}
	}

	child = ExtractAggregateExpressions(std::move(child), aggregates, groups);

	// we add a physical hash aggregation in the plan to select the distinct groups
	auto groupby = make_uniq<PhysicalHashAggregate>(context, aggregate_types, std::move(aggregates), std::move(groups),
	                                                child->estimated_cardinality);
	groupby->children.push_back(std::move(child));
	if (!requires_projection) {
		return std::move(groupby);
	}

	// we add a physical projection on top of the aggregation to project all members in the select list
	auto aggr_projection = make_uniq<PhysicalProjection>(types, std::move(projections), groupby->estimated_cardinality);
	aggr_projection->children.push_back(std::move(groupby));
	return std::move(aggr_projection);
}

} // namespace duckdb
