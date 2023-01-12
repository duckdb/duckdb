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
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	AccessMode access_mode = config.options.access_mode;
	for (auto &entry : info->options) {
		if (entry.first == "readonly" || entry.first == "read_only") {
			auto read_only = BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else if (entry.first == "read_write" || entry.first == "read_write") {
			auto read_only = !BooleanValue::Get(entry.second.DefaultCastAs(LogicalType::BOOLEAN));
			if (read_only) {
				access_mode = AccessMode::READ_ONLY;
			} else {
				access_mode = AccessMode::READ_WRITE;
			}
		} else {
			throw BinderException("Unrecognized option for attach \"%s\"", entry.first);
		}
	}
	// attach the database
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
	auto new_db = make_unique<AttachedDatabase>(db, Catalog::GetSystemCatalog(context.client), name, path, access_mode);
	new_db->Initialize();

	db_manager.AddDatabase(context.client, std::move(new_db));
	state.finished = true;
}

} // namespace duckdb
