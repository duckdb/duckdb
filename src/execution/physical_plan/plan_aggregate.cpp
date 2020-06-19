#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate &op) {
	unique_ptr<PhysicalOperator> groupby;
	assert(op.children.size() == 1);

	bool all_combinable = true;
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*op.expressions[i];
		if (!aggregate.function.combine) {
			// unsupported aggregate for simple aggregation: use hash aggregation
			all_combinable = false;
			break;
		}
	}

	auto plan = CreatePlan(*op.children[0]);
	if (op.groups.size() == 0) {
		// no groups, check if we can use a simple aggregation
		// special case: aggregate entire columns together
		bool use_simple_aggregation = true;
		for (idx_t i = 0; i < op.expressions.size(); i++) {
			auto &aggregate = (BoundAggregateExpression &)*op.expressions[i];
			if (!aggregate.function.simple_update || aggregate.distinct) {
				// unsupported aggregate for simple aggregation: use hash aggregation
				use_simple_aggregation = false;
				break;
			}
		}
		if (use_simple_aggregation) {
			groupby = make_unique_base<PhysicalOperator, PhysicalSimpleAggregate>(op.types, move(op.expressions),
			                                                                      all_combinable);
		} else {
			groupby = make_unique_base<PhysicalOperator, PhysicalHashAggregate>(op.types, move(op.expressions));
		}
	} else {
		// groups! create a GROUP BY aggregator
		groupby =
		    make_unique_base<PhysicalOperator, PhysicalHashAggregate>(op.types, move(op.expressions), move(op.groups));
	}
	groupby->children.push_back(move(plan));
	return groupby;
}
