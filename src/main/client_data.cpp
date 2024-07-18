#include "duckdb/main/client_data.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

class ClientFileSystem : public OpenerFileSystem {
public:
	explicit ClientFileSystem(ClientContext &context_p) : context(context_p) {
	}

	FileSystem &GetFileSystem() const override {
		auto &config = DBConfig::GetConfig(context);
		return *config.file_system;
	}

	optional_ptr<FileOpener> GetOpener() const override {
		return ClientData::Get(context).file_opener.get();
	}

private:
	ClientContext &context;
};

ClientData::ClientData(ClientContext &context) : catalog_search_path(make_uniq<CatalogSearchPath>(context)) {
	auto &db = DatabaseInstance::GetDatabase(context);
	profiler = make_shared_ptr<QueryProfiler>(context);
	http_logger = make_shared_ptr<HTTPLogger>(context);
	temporary_objects = make_shared_ptr<AttachedDatabase>(db, AttachedDatabaseType::TEMP_DATABASE);
	temporary_objects->oid = DatabaseManager::Get(db).NextOid();
	random_engine = make_uniq<RandomEngine>();
	file_opener = make_uniq<ClientContextFileOpener>(context);
	client_file_system = make_uniq<ClientFileSystem>(context);
	temporary_objects->Initialize(DEFAULT_BLOCK_ALLOC_SIZE);
}

ClientData::~ClientData() {
}

ClientData &ClientData::Get(ClientContext &context) {
	return *context.client_data;
}

const ClientData &ClientData::Get(const ClientContext &context) {
	return *context.client_data;
}

RandomEngine &RandomEngine::Get(ClientContext &context) {
	return *ClientData::Get(context).random_engine;
}

} // namespace duckdb
