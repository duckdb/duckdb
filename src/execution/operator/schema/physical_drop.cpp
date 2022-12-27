#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"

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
	case CatalogType::DATABASE_ENTRY: {
		auto &db_manager = DatabaseManager::Get(context.client);
		db_manager.DetachDatabase(context.client, info->name, info->if_exists);
		break;
	}
	default: {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.DropEntry(context.client, info.get());
		break;
	}
	}
	state.finished = true;
}

} // namespace duckdb
