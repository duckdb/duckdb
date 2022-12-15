#include "duckdb/execution/operator/schema/physical_attach.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class AttachSourceState : public GlobalSourceState {
public:
	AttachSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalAttach::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<AttachSourceState>();
}

void PhysicalAttach::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto &state = (AttachSourceState &)gstate;
	if (state.finished) {
		return;
	}
	auto name = info->name;
	auto path = info->path;
	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (name.empty()) {
		name = AttachedDatabase::ExtractDatabaseName(path);
	}
	auto &db_manager = DatabaseManager::Get(context.client);
	auto existing_db = db_manager.GetDatabaseFromPath(context.client, path);
	if (existing_db) {
		throw BinderException("Database \"%s\" is already attached with alias \"%s\"", path, existing_db->GetName());
	}
	auto new_db =
	    make_unique<AttachedDatabase>(db, Catalog::GetSystemCatalog(context.client), name, path, AccessMode::READ_WRITE);
	new_db->Initialize();

	db_manager.AddDatabase(context.client, move(new_db));
	state.finished = true;
}

} // namespace duckdb
