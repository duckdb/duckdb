#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(op);

	// get relation_stats here since the reconstruction process will move all of the relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;
	if (reorderable) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);

		// Initialize a plan enumerator.
		auto plan_enumerator = PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraph());

		// Initialize the leaf/single node plans
		// TODO: the translation of SingleJoinNodeRelations to JoinNode should not happen in the
		//  enumerator. The enumerator
		plan_enumerator.InitLeafPlans();

		// Ask the plan enumerator to enumerate a number of join orders
		auto final_plan = plan_enumerator.SolveJoinOrder(context.config.force_no_cross_product);
		// TODO: add in the check that if no plan exists, you have to add a cross product.

		// now reconstruct a logical plan from the query graph plan
		new_logical_plan = query_graph_manager.Reconstruct(std::move(plan), *final_plan);
	} else {
		new_logical_plan = std::move(plan);
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		idx_t max_card = 0;
		for (auto &child_stats: relation_stats) {
			for (idx_t i = 0; i < child_stats.column_distinct_count.size(); i++) {
				stats->column_distinct_count.push_back(child_stats.column_distinct_count.at(i));
				stats->column_names.push_back(child_stats.column_names.at(i));
			}
			stats->table_name += "joined with " + child_stats.table_name;
			max_card = MaxValue(max_card, child_stats.cardinality);
		}
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		stats->cardinality = MaxValue(cardinality, max_card);
		stats->filter_strength = 1;
		stats->stats_initialized = true;
	}

	return new_logical_plan;
}

} // namespace duckdb
