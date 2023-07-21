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

	if (reorderable) {
		// at most one relation, nothing to reorder
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

	// Ask the plan enumerator to enumerate a number of join orders
	auto final_plan = plan_enumerator.SolveJoinOrder(context.config.force_no_cross_product);
	// TODO: add in the check that if no plan exists, you have to add a cross product.

	// now reconstruct a logical plan from the query graph plan
	auto new_logical_plan = query_graph_manager.Reconstruct(std::move(plan), *final_plan);

	// TODO: swap left and right operators for any joins based on statistics. This is no longer handled by the
	// enumerator.
	//  as left and right positioning is not logic that should be in the enumerator
	//  below code ripped from extract join relations.
	//	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
	//			auto &join = op->Cast<LogicalComparisonJoin>();
	//			if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
	//				// for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
	//				// we convert to doing a RIGHT OUTER JOIN
	//				// FIXME: for now we don't swap if the right_projection_map is not empty
	//				// this can be fixed once we implement the left_projection_map properly...
	//				auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
	//				auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
	//				if (rhs_cardinality > lhs_cardinality * 2) {
	//					join.join_type = JoinType::RIGHT;
	//					std::swap(join.children[0], join.children[1]);
	//					for (auto &cond : join.conditions) {
	//						std::swap(cond.left, cond.right);
	//						cond.comparison = FlipComparisonExpression(cond.comparison);
	//					}
	//				}
	//			}
	//		}
	//	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN && non_reorderable_operation) {
	//		auto &join = op->Cast<LogicalAnyJoin>();
	//		if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
	//			auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
	//			auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
	//			if (rhs_cardinality > lhs_cardinality * 2) {
	//				join.join_type = JoinType::RIGHT;
	//				std::swap(join.children[0], join.children[1]);
	//			}
	//		}
	//	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		vector<idx_t> distinct_column_counts;
		for (auto &relation: query_graph_manager.relation_manager.GetRelations()) {
			for (auto &distinct_column_count : relation.stats.column_distinct_count) {
				distinct_column_counts.push_back(distinct_column_count);
			}
		}
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		stats->column_distinct_count = distinct_column_counts;
		stats->cardinality = cardinality;
		stats->filter_strength = 1;
		stats->stats_initialized = true;
		// TODO: some verification logic.
	}

	return new_logical_plan;
}

} // namespace duckdb
