#include "duckdb/execution/operator/schema/physical_create_table_as.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateTableAs::PhysicalCreateTableAs(LogicalOperator &op, SchemaCatalogEntry *schema,
                                             unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), schema(schema),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTableAsGlobalState : public GlobalSinkState {
public:
	CreateTableAsGlobalState() : inserted_count(0), initialized(false) {
	}

	mutex append_lock;
	TableCatalogEntry *table;
	int64_t inserted_count;
	LocalAppendState append_state;
	bool initialized;
};

unique_ptr<GlobalSinkState> PhysicalCreateTableAs::GetGlobalSinkState(ClientContext &context) const {
	auto sink = make_unique<CreateTableAsGlobalState>();
	auto &catalog = Catalog::GetCatalog(context);
	sink->table = (TableCatalogEntry *)catalog.CreateTable(context, schema, info.get());
	return move(sink);
}

SinkResultType PhysicalCreateTableAs::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                           DataChunk &input) const {
	auto &sink = (CreateTableAsGlobalState &)state;
	D_ASSERT(sink.table);
	lock_guard<mutex> client_guard(sink.append_lock);
	if (!sink.initialized) {
		sink.table->storage->InitializeLocalAppend(sink.append_state, context.client);
		sink.initialized = true;
	}
	sink.table->storage->LocalAppend(sink.append_state, *sink.table, context.client, input);
	sink.inserted_count += input.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalCreateTableAs::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &state) const {
	auto &gstate = (CreateTableAsGlobalState &)state;
	if (gstate.initialized) {
		gstate.table->storage->FinalizeLocalAppend(gstate.append_state);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTableAsSourceState : public GlobalSourceState {
public:
	CreateTableAsSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateTableAs::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTableAsSourceState>();
}

void PhysicalCreateTableAs::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                    LocalSourceState &lstate) const {
	auto &state = (CreateTableAsSourceState &)gstate;
	auto &sink = (CreateTableAsGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	if (sink.table) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(sink.inserted_count));
	}
	state.finished = true;
}

} // namespace duckdb
