#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateViewSourceState : public GlobalSourceState {
public:
	CreateViewSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateView::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateViewSourceState>();
}

void PhysicalCreateView::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateViewSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateView(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
