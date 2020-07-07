#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

namespace duckdb {

void PhysicalAlter::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto table_info = (AlterTableInfo *)info.get();
	Catalog::GetCatalog(context.client).AlterTable(context.client, table_info);
	state->finished = true;
}

} // namespace duckdb
