#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

OrderPreservationType PhysicalPlanGenerator::OrderPreservationRecursive(PhysicalOperator &op) {
	if (op.IsSource()) {
		return op.SourceOrder();
	}

	idx_t child_idx = 0;
	for (auto &child : op.children) {
		// Do not take the materialization phase of physical CTEs into account
		if (op.type == PhysicalOperatorType::CTE && child_idx == 0) {
			continue;
		}
		auto child_preservation = OrderPreservationRecursive(*child);
		if (child_preservation != OrderPreservationType::INSERTION_ORDER) {
			return child_preservation;
		}
		child_idx++;
	}
	return OrderPreservationType::INSERTION_ORDER;
}

bool PhysicalPlanGenerator::PreserveInsertionOrder(ClientContext &context, PhysicalOperator &plan) {
	auto &config = DBConfig::GetConfig(context);

	auto preservation_type = OrderPreservationRecursive(plan);
	if (preservation_type == OrderPreservationType::FIXED_ORDER) {
		// always need to maintain preservation order
		return true;
	}
	if (preservation_type == OrderPreservationType::NO_ORDER) {
		// never need to preserve order
		return false;
	}
	// preserve insertion order - check flags
	if (!config.options.preserve_insertion_order) {
		// preserving insertion order is disabled by config
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

unique_ptr<PhysicalOperator> DuckCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                     unique_ptr<PhysicalOperator> plan) {
	bool parallel_streaming_insert = !PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
	bool use_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, *plan);
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	if (op.return_chunk) {
		// not supported for RETURNING (yet?)
		parallel_streaming_insert = false;
		use_batch_index = false;
	}
	if (op.action_type != OnConflictAction::THROW) {
		// We don't support ON CONFLICT clause in batch insertion operation currently
		use_batch_index = false;
	}
	if (op.action_type == OnConflictAction::UPDATE) {
		// When we potentially need to perform updates, we have to check that row is not updated twice
		// that currently needs to be done for every chunk, which would add a huge bottleneck to parallelized insertion
		parallel_streaming_insert = false;
	}
	unique_ptr<PhysicalOperator> insert;
	if (use_batch_index && !parallel_streaming_insert) {
		insert = make_uniq<PhysicalBatchInsert>(op.types, op.table, op.column_index_map, std::move(op.bound_defaults),
		                                        std::move(op.bound_constraints), op.estimated_cardinality);
	} else {
		insert = make_uniq<PhysicalInsert>(
		    op.types, op.table, op.column_index_map, std::move(op.bound_defaults), std::move(op.bound_constraints),
		    std::move(op.expressions), std::move(op.set_columns), std::move(op.set_types), op.estimated_cardinality,
		    op.return_chunk, parallel_streaming_insert && num_threads > 1, op.action_type,
		    std::move(op.on_conflict_condition), std::move(op.do_update_condition), std::move(op.on_conflict_filter),
		    std::move(op.columns_to_fetch), op.update_is_del_and_insert);
	}
	D_ASSERT(plan);
	insert->children.push_back(std::move(plan));
	return insert;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalInsert &op) {
	unique_ptr<PhysicalOperator> plan;
	if (!op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		plan = CreatePlan(*op.children[0]);
	}
	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanInsert(context, op, std::move(plan));
}

} // namespace duckdb
