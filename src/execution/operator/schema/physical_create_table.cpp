#include "duckdb/execution/operator/schema/physical_create_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalCreateTable::PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema,
                                         unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE, op.types, estimated_cardinality), schema(schema),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTableSourceState : public GlobalSourceState {
public:
	CreateTableSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateTable::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTableSourceState>();
}

void PhysicalCreateTable::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &state = (CreateTableSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, INVALID_CATALOG);
	catalog.CreateTable(context.client, schema, info.get());
	state.finished = true;
}

} // namespace duckdb
