#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateTable &op) {
	auto &create_info = (CreateTableInfo &)*op.info->base;
	auto existing_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, create_info.schema, create_info.table, true);
	bool replace = op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT;
	if ((!existing_entry || replace) && !op.children.empty()) {
		auto plan = CreatePlan(*op.children[0]);

		bool parallel_streaming_insert = !PreserveInsertionOrder(*plan);
		bool use_batch_index = UseBatchIndex(*plan);
		auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		unique_ptr<PhysicalOperator> create;
		if (!parallel_streaming_insert && use_batch_index) {
			create = make_unique<PhysicalBatchInsert>(op, op.schema, move(op.info), op.estimated_cardinality);

		} else {
			create = make_unique<PhysicalInsert>(op, op.schema, move(op.info), op.estimated_cardinality,
			                                     parallel_streaming_insert && num_threads > 1);
		}

		D_ASSERT(op.children.size() == 1);
		create->children.push_back(move(plan));
		return create;
	} else {
		return make_unique<PhysicalCreateTable>(op, op.schema, move(op.info), op.estimated_cardinality);
	}
}

} // namespace duckdb
