#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DropSourceState : public GlobalSourceState {
public:
	DropSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDrop::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<DropSourceState>();
}

void PhysicalDrop::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                           LocalSourceState &lstate) const {
	auto &state = (DropSourceState &)gstate;
	if (state.finished) {
		return;
	}
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT: {
		// DEALLOCATE silently ignores errors
		auto &statements = ClientData::Get(context.client).prepared_statements;
		if (statements.find(info->name) != statements.end()) {
			statements.erase(info->name);
		}
		break;
	}
	default:
		Catalog::GetCatalog(context.client).DropEntry(context.client, info.get());
		break;
	}
	state.finished = true;
}

} // namespace duckdb
