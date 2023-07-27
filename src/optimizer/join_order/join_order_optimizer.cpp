#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/queue.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize and non-reorderable relations we come across.
	bool reorderable = query_graph_manager.Build(op);
	
	if (!reorderable) {
		// at most one relation, nothing to reorder
		// copy stats
		if (stats) {
			auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
			// if it's non reorderable, we should have 0 or 1 relation. This is because other non-reorderable
			// relations (like left joins), will have their children optimized, and the relation will then be the
			// join. One relation is non-reorderable as well, then we end up here.
			D_ASSERT(relation_stats.size() <= 1);
			idx_t max_card = 0;
			for (auto &child_stats: relation_stats) {
				for (idx_t i = 0; i < child_stats.column_distinct_count.size(); i++) {
					stats->column_distinct_count.push_back(child_stats.column_distinct_count.at(i));
					stats->column_names.push_back(child_stats.column_names.at(i));
				}
				stats->table_name += child_stats.table_name;
				max_card = MaxValue(max_card, child_stats.cardinality);
			}
			auto cardinality = plan->EstimateCardinality(context);
			stats->cardinality = MaxValue(cardinality, max_card);
			stats->filter_strength = 1;
			stats->stats_initialized = true;

			// TODO: some verification logic. make sure column names are the same
			//  combine table names.
		}
		return plan;
	}

	// query graph now has filters and relations
	auto cost_model = CostModel(query_graph_manager);

	// Initialize a plan enumerator.
	auto plan_enumerator = PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraph());

	// Initialize the leaf/single node plans
	// TODO: the translation of SingleJoinNodeRelations to JoinNode should not happen in the
	//  enumerator. The enumerator
	plan_enumerator.InitLeafPlans();

//	cost_model.cardinality_estimator.PrintRelationToTdomInfo();

	// Ask the plan enumerator to enumerate a number of join orders
	auto final_plan = plan_enumerator.SolveJoinOrder(context.config.force_no_cross_product);
	// TODO: add in the check that if no plan exists, you have to add a cross product.

	// get relation_stats here since the reconstruction process will move all of the relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();

	// now reconstruct a logical plan from the query graph plan
	auto new_logical_plan = query_graph_manager.Reconstruct(std::move(plan), *final_plan);

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
		// TODO: some verification logic. make sure column names are the same
		//  combine table names.
	}

	return new_logical_plan;
}

} // namespace duckdb
