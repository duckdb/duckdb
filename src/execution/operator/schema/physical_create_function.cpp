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
	return make_uniq<CreateFunctionSourceState>();
}

void PhysicalCreateFunction::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = gstate.Cast<CreateFunctionSourceState>();
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateFunction(context.client, *info);
	state.finished = true;
}

} // namespace duckdb
