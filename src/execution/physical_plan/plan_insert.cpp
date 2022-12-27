#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

bool PhysicalPlanGenerator::PreserveInsertionOrder(ClientContext &context, PhysicalOperator &plan) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.preserve_insertion_order) {
		// preserving insertion order is disabled by config
		return false;
	}
	if (!plan.AllOperatorsPreserveOrder()) {
		// the plan has no order defined: no need to preserve insertion order
		return false;
	}
	return true;
}

bool PhysicalPlanGenerator::PreserveInsertionOrder(PhysicalOperator &plan) {
	return PreserveInsertionOrder(context, plan);
}

bool PhysicalPlanGenerator::UseBatchIndex(ClientContext &context, PhysicalOperator &plan) {
	// TODO: always preserve order if query contains ORDER BY
	auto &scheduler = TaskScheduler::GetScheduler(context);
	if (scheduler.NumberOfThreads() == 1) {
		// batch index usage only makes sense if we are using multiple threads
		return false;
	}
	if (!plan.AllSourcesSupportBatchIndex()) {
		// batch index is not supported
		return false;
	}
	return true;
}

bool PhysicalPlanGenerator::UseBatchIndex(PhysicalOperator &plan) {
	return UseBatchIndex(context, plan);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalInsert &op) {
	unique_ptr<PhysicalOperator> plan;
	if (!op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		plan = CreatePlan(*op.children[0]);
	}
	dependencies.insert(op.table);

	// FIXME: should this not assert `plan` is not nullptr?
	bool parallel_streaming_insert = !PreserveInsertionOrder(*plan);
	bool use_batch_index = UseBatchIndex(*plan);
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	if (op.return_chunk) {
		// not supported for RETURNING (yet?)
		parallel_streaming_insert = false;
		use_batch_index = false;
	}
	unique_ptr<PhysicalOperator> insert;
	if (use_batch_index && !parallel_streaming_insert) {
		insert = make_unique<PhysicalBatchInsert>(op.types, op.table, op.column_index_map, move(op.bound_defaults),
		                                          op.estimated_cardinality);
	} else {
		insert = make_unique<PhysicalInsert>(op.types, op.table, op.column_index_map, move(op.bound_defaults),
		                                     op.estimated_cardinality, op.return_chunk,
		                                     parallel_streaming_insert && num_threads > 1, op.action_type);
	}
	if (plan) {
		// FIXME: same as above
		insert->children.push_back(move(plan));
	}
	return insert;
}

} // namespace duckdb
