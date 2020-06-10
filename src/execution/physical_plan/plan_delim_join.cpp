#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
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
		hash_join.delim_types = delim_types;
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
	auto distinct = CreateDistinct(move(projection));
	assert(distinct->type == PhysicalOperatorType::DISTINCT);
	delim_join->distinct = unique_ptr_cast<PhysicalOperator, PhysicalHashAggregate>(move(distinct));
	return move(delim_join);
}
