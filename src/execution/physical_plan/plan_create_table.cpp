#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> DuckCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                            unique_ptr<PhysicalOperator> plan) {
	bool parallel_streaming_insert = !PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
	bool use_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, *plan);
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	unique_ptr<PhysicalOperator> create;
	if (!parallel_streaming_insert && use_batch_index) {
		create = make_uniq<PhysicalBatchInsert>(op, op.schema, std::move(op.info), 0U);

	} else {
		create = make_uniq<PhysicalInsert>(op, op.schema, std::move(op.info), 0U,
		                                   parallel_streaming_insert && num_threads > 1);
	}

	D_ASSERT(op.children.size() == 1);
	create->children.push_back(std::move(plan));
	return create;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateTable &op) {
	const auto &create_info = op.info->base->Cast<CreateTableInfo>();
	auto &catalog = op.info->schema.catalog;
	auto existing_entry = catalog.GetEntry<TableCatalogEntry>(context, create_info.schema, create_info.table,
	                                                          OnEntryNotFound::RETURN_NULL);
	bool replace = op.info->Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT;
	if ((!existing_entry || replace) && !op.children.empty()) {
		auto plan = CreatePlan(*op.children[0]);
		return op.schema.catalog.PlanCreateTableAs(context, op, std::move(plan));
	} else {
		return make_uniq<PhysicalCreateTable>(op, op.schema, std::move(op.info), op.estimated_cardinality);
	}
}

} // namespace duckdb
