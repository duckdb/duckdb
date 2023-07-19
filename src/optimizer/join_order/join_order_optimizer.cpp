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
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {

	// make sure query graph manager has not extracted a relation graph already

//	D_ASSERT(!query_graph_manager.HasQueryGraph())
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize and non-reorderable relations we come across.
	query_graph_manager.Build(op);

	if (query_graph_manager.relation_manager.NumRelations() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}

	//! QUERY GRAPH NOW HAS FILTERS AND RELATIONS KEWL

	auto cost_model = CostModel(query_graph_manager);

	// Initialize a plan enumerator
	auto plan_enumerator = PlanEnumerator(query_graph_manager, cost_model);

	// Ask the plan enumerator to enumerate a number of join orders
	auto final_plan = plan_enumerator.SolveJoinOrder();

	// now reconstruct a logical plan from the query graph plan
	auto new_logical_plan = query_graph_manager.Reconstruct(final_plan);

	// TODO: swap left and right joins based on statistics.

	return std::move(new_logical_plan);
}

} // namespace duckdb
