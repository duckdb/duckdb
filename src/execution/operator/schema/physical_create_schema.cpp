#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateSchemaSourceState : public GlobalSourceState {
public:
	CreateSchemaSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateSchema::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreateSchemaSourceState>();
}

void PhysicalCreateSchema::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                   LocalSourceState &lstate) const {
	auto &state = gstate.Cast<CreateSchemaSourceState>();
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	if (catalog.IsSystemCatalog()) {
		throw BinderException("Cannot create schema in system catalog");
	}
	catalog.CreateSchema(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
