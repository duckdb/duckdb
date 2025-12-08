#include "duckdb/execution/operator/schema/physical_create_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateTable::PhysicalCreateTable(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                                         unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_TABLE, op.types, estimated_cardinality),
      schema(schema), info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateTable::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &catalog = schema.catalog;
	catalog.CreateTable(catalog.GetCatalogTransaction(context.client), schema, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
