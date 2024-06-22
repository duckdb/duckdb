#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                         optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*op);

	// get relation_stats here since the reconstruction process will move all relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;

	if (reorderable) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);

		// Initialize a plan enumerator.
		auto plan_enumerator =
		    PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

		// Initialize the leaf/single node plans
		plan_enumerator.InitLeafPlans();
		plan_enumerator.SolveJoinOrder();
		// now reconstruct a logical plan from the query graph plan
		query_graph_manager.plans = &plan_enumerator.GetPlans();

		new_logical_plan = query_graph_manager.Reconstruct(std::move(plan));
	} else {
		new_logical_plan = std::move(plan);
		if (relation_stats.size() == 1) {
			new_logical_plan->estimated_cardinality = relation_stats.at(0).cardinality;
			new_logical_plan->has_estimated_cardinality = true;
		}
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		auto bindings = new_logical_plan->GetColumnBindings();
		auto new_stats = RelationStatisticsHelper::CombineStatsOfReorderableOperator(bindings, relation_stats);
		new_stats.cardinality = cardinality;
		RelationStatisticsHelper::CopyRelationStats(*stats, new_stats);
	}

	return new_logical_plan;
}

} // namespace duckdb
