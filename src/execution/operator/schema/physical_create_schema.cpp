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
	return make_unique<CreateSchemaSourceState>();
}

void PhysicalCreateSchema::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                   LocalSourceState &lstate) const {
	auto &state = (CreateSchemaSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateSchema(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
