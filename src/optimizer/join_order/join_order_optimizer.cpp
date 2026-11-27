#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

JoinOrderOptimizer::JoinOrderOptimizer(ClientContext &context)
    : context(context), query_graph_manager(context), depth(1) {
}

JoinOrderOptimizer JoinOrderOptimizer::CreateChildOptimizer() {
	JoinOrderOptimizer child_optimizer(context);
	child_optimizer.materialized_cte_stats = materialized_cte_stats;
	child_optimizer.delim_scan_stats = delim_scan_stats;
	child_optimizer.depth = depth + 1;
	child_optimizer.recursive_cte_indexes = recursive_cte_indexes;
	child_optimizer.source_stats = source_stats;
	return child_optimizer;
}

const shared_ptr<const JoinOrderSourceStats> &JoinOrderOptimizer::GetSourceStats() const {
	return source_stats;
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                         optional_ptr<RelationStats> stats) {
	if (depth > MAX_RECURSION_DEPTH) {
		// Very deep plans will eventually consume quite some stack space
		// Returning the current plan is always a valid choice
		return plan;
	}

	// make sure query graph manager has not extracted a relation graph already
	auto &op = *plan;
	if (!source_stats) {
		source_stats = make_shared_ptr<JoinOrderSourceStats>(context, op);
	}

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*this, op);

	unique_ptr<LogicalOperator> new_logical_plan = nullptr;
	RelationStats output_stats;

	auto &relation_manager = query_graph_manager.relation_manager;
	if (reorderable || (relation_manager.NumRelations() > 1 && relation_manager.HasCompleteStats())) {
		// A retained join order still has an estimable region when all inputs were captured.
		auto cardinality_estimator =
		    CardinalityEstimator(query_graph_manager.set_manager, query_graph_manager.GetPredicateModel());
		if (reorderable) {
			auto cost_model = CostModel(cardinality_estimator);
			bool inner_region = true;
			for (auto &join : query_graph_manager.GetJoinOperators()) {
				inner_region &=
				    join->type == JoinOrderOperatorType::INNER || join->type == JoinOrderOperatorType::CROSS_PRODUCT;
			}
			if (inner_region) {
				cost_model.InitializeRecursiveMemory(context, relation_manager);
			}

			// Initialize a plan enumerator.
			auto plan_enumerator =
			    PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

			// Initialize the leaf/single node plans
			plan_enumerator.InitLeafPlans();
			if (plan_enumerator.SolveJoinOrder()) {
				// now reconstruct a logical plan from the query graph plan
				query_graph_manager.plans = plan_enumerator.GetPlans();
				auto reconstructed = query_graph_manager.Reconstruct(std::move(plan), cardinality_estimator);
				new_logical_plan = std::move(reconstructed.op);
				output_stats = std::move(reconstructed.stats);
			}
		} else {
			cardinality_estimator.Initialize(relation_manager.GetRelationStats(),
			                                 query_graph_manager.GetJoinOperators(),
			                                 relation_manager.GetColumnSources());
		}
		if (!new_logical_plan) {
			unordered_set<RelationIndex> relations;
			for (idx_t i = 0; i < relation_manager.NumRelations(); i++) {
				relations.insert(RelationIndex(i));
			}
			auto &set = query_graph_manager.set_manager.GetJoinRelation(relations);
			auto projected =
			    query_graph_manager.ProjectRetainedStats(cardinality_estimator, set, op, PlanEstimateUpdate::UPDATE);
			if (projected) {
				output_stats = std::move(*projected);
			}
		}
	}
	if (!new_logical_plan) {
		// Keep the original tree when there is nothing to reorder or enumeration cannot complete.
		new_logical_plan = std::move(plan); // NOLINT: plan is only moved when reconstruction succeeded
		if (!reorderable && relation_manager.NumRelations() == 1) {
			auto &relation_stats = relation_manager.GetRelation(RelationIndex(0)).stats;
			new_logical_plan->SetEstimatedCardinality(relation_stats.GetCardinalityForPlan());
			if (relation_manager.HasCompleteStats()) {
				auto combined = RelationStatisticsHelper::ProjectOutputStats(relation_stats, *new_logical_plan);
				if (combined) {
					output_stats = std::move(*combined);
				}
			}
		}
		if (output_stats.stats_initialized) {
			new_logical_plan->SetEstimatedCardinality(output_stats.GetCardinalityForPlan());
		}
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		*stats = std::move(output_stats);
	}

	if (new_logical_plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		new_logical_plan->SetEstimatedCardinality(3);
	}

	return new_logical_plan;
}

void JoinOrderOptimizer::AddMaterializedCTEStats(TableIndex index, RelationStats &&stats) {
	materialized_cte_stats.emplace(index, std::move(stats));
}

RelationStats JoinOrderOptimizer::GetMaterializedCTEStats(TableIndex index) {
	auto it = materialized_cte_stats.find(index);
	if (it == materialized_cte_stats.end()) {
		throw InternalException("Unable to find materialized CTE stats with index %llu", index.index);
	}
	return it->second;
}

void JoinOrderOptimizer::AddDelimScanStats(RelationStats &stats) {
	delim_scan_stats = &stats;
}

RelationStats JoinOrderOptimizer::GetDelimScanStats() {
	if (!delim_scan_stats) {
		throw InternalException("Unable to find delim scan stats!");
	}
	return *delim_scan_stats;
}

} // namespace duckdb
