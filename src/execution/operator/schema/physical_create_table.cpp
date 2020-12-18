#include "duckdb/execution/operator/schema/physical_create_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace std;

namespace duckdb {

PhysicalCreateTable::PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info)
    : PhysicalSink(PhysicalOperatorType::CREATE, op.types), schema(schema), info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTableGlobalState : public GlobalOperatorState {
public:
	CreateTableGlobalState() {
	}
	ChunkCollection materialized_child;
};

unique_ptr<GlobalOperatorState> PhysicalCreateTable::GetGlobalState(ClientContext &context) {
	return make_unique<CreateTableGlobalState>();
}

void PhysicalCreateTable::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_,
                               DataChunk &input) {
	lock_guard<mutex> client_guard(append_lock);
	auto &sink = (CreateTableGlobalState &)state;
	sink.materialized_child.Append(input);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalCreateTable::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto table = (TableCatalogEntry *)schema->CreateTable(context.client, info.get());
	if (table && children.size() > 0) {
		// CREATE TABLE AS
		auto &sink = (CreateTableGlobalState &)*sink_state;
		for (auto &chunk : sink.materialized_child.Chunks()) {
			table->storage->Append(*table, context.client, *chunk);
		}
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(sink.materialized_child.Count()));
	}

	state->finished = true;
}

} // namespace duckdb
