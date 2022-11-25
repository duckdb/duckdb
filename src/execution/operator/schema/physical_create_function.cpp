#include "duckdb/execution/operator/schema/physical_create_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateFunctionSourceState : public GlobalSourceState {
public:
	CreateFunctionSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateFunction::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateFunctionSourceState>();
}

void PhysicalCreateFunction::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (CreateFunctionSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, INVALID_CATALOG);
	catalog.CreateFunction(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
