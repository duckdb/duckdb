#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

using namespace std;

namespace duckdb {

void PhysicalAlter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto table_info = (AlterTableInfo *)info.get();
	context.catalog.AlterTable(context, table_info);
	state->finished = true;
}

} // namespace duckdb
