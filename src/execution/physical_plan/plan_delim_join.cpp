#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

static void GatherDelimScans(PhysicalOperator *op, vector<PhysicalOperator *> &delim_scans) {
	assert(op);
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto &child : op->children) {
		GatherDelimScans(child.get(), delim_scans);
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimJoin &op) {
	// first create the underlying join
	auto plan = CreatePlan((LogicalComparisonJoin &)op);
	// this should create a join, not a cross product
	assert(plan && plan->type != PhysicalOperatorType::CROSS_PRODUCT);
	// duplicate eliminated join
	// first gather the scans on the duplicate eliminated data set from the RHS
	vector<PhysicalOperator *> delim_scans;
	GatherDelimScans(plan->children[1].get(), delim_scans);
	if (delim_scans.size() == 0) {
		// no duplicate eliminated scans in the RHS!
		// in this case we don't need to create a delim join
		// just push the normal join
		return plan;
	}
	vector<TypeId> delim_types;
	for (auto &delim_expr : op.duplicate_eliminated_columns) {
		delim_types.push_back(delim_expr->return_type);
	}
	if (op.join_type == JoinType::MARK) {
		assert(plan->type == PhysicalOperatorType::HASH_JOIN);
		auto &hash_join = (PhysicalHashJoin &)*plan;
		// correlated MARK join
		if (delim_types.size() + 1 == hash_join.conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = hash_join.hash_table->correlated_mark_join_info;

			vector<TypeId> payload_types = {TypeId::INT64, TypeId::INT64}; // COUNT types
			vector<AggregateFunction> aggregate_functions = {CountStarFun::GetFunction(), CountFun::GetFunction()};
			vector<BoundAggregateExpression *> correlated_aggregates;
			for (idx_t i = 0; i < aggregate_functions.size(); ++i) {
				auto aggr = make_unique<BoundAggregateExpression>(payload_types[i], aggregate_functions[i], false);
				correlated_aggregates.push_back(&*aggr);
				info.correlated_aggregates.push_back(move(aggr));
			}
			info.correlated_counts =
			    make_unique<SuperLargeHashTable>(1024, delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			// FIXME: these can be initialized "empty" (without allocating empty vectors)
			info.group_chunk.Initialize(delim_types);
			info.payload_chunk.Initialize(payload_types);
			info.result_chunk.Initialize(payload_types);
		}
	}
	// now create the duplicate eliminated join
	auto delim_join = make_unique<PhysicalDelimJoin>(op, move(plan), delim_scans);
	// we still have to create the DISTINCT clause that is used to generate the duplicate eliminated chunk
	// we create a ChunkCollectionScan that pulls from the delim_join LHS
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(delim_join->children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->collection = &delim_join->lhs_data;
	// now we need to create a projection that projects only the duplicate eliminated columns
	assert(op.duplicate_eliminated_columns.size() > 0);
	auto projection = make_unique<PhysicalProjection>(delim_types, move(op.duplicate_eliminated_columns));
	projection->children.push_back(move(chunk_scan));
	// finally create the distinct clause on top of the projection
	delim_join->distinct = CreateDistinct(move(projection));
	return move(delim_join);
}
