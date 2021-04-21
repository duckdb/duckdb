#include "duckdb/execution/operator/schema/physical_create_table_as.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateTableAs::PhysicalCreateTableAs(LogicalOperator &op, SchemaCatalogEntry *schema,
                                             unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), schema(schema),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTableAsGlobalState : public GlobalOperatorState {
public:
	CreateTableAsGlobalState() {
		inserted_count = 0;
	}
	mutex append_lock;
	TableCatalogEntry *table;
	int64_t inserted_count;
};

unique_ptr<GlobalOperatorState> PhysicalCreateTableAs::GetGlobalState(ClientContext &context) {
	auto sink = make_unique<CreateTableAsGlobalState>();
	auto &catalog = Catalog::GetCatalog(context);
	sink->table = (TableCatalogEntry *)catalog.CreateTable(context, schema, info.get());
	return move(sink);
}

void PhysicalCreateTableAs::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                                 DataChunk &input) {
	auto &sink = (CreateTableAsGlobalState &)state;
	if (sink.table) {
		lock_guard<mutex> client_guard(sink.append_lock);
		sink.table->storage->Append(*sink.table, context.client, input);
		sink.inserted_count += input.size();
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalCreateTableAs::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state) {
	auto &sink = (CreateTableAsGlobalState &)*sink_state;
	if (sink.table) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(sink.inserted_count));
	}
	state->finished = true;
}

} // namespace duckdb
