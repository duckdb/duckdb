#include "duckdb/execution/operator/schema/physical_create_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateTable::PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE, op.types), schema(schema), info(move(info)) {
}

void PhysicalCreateTable::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	schema->CreateTable(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
