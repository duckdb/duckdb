#include "main/database.hpp"

#include "catalog/catalog.hpp"
#include "common/file_system.hpp"
#include "main/connection_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

DBConfig::~DBConfig() {
}

DuckDB::DuckDB(const char *path, DBConfig *config) {
	if (config) {
		// user-supplied configuration
		Configure(*config);
	} else {
		// default configuration
		DBConfig config;
		Configure(config);
	}

	storage = make_unique<StorageManager>(*this, path ? string(path) : string(), access_mode == AccessMode::READ_ONLY);
	catalog = make_unique<Catalog>(*storage);
	transaction_manager = make_unique<TransactionManager>(*storage);
	connection_manager = make_unique<ConnectionManager>();
	// initialize the database
	storage->Initialize();
}

DuckDB::DuckDB(const string &path, DBConfig *config) : DuckDB(path.c_str(), config) {
}

DuckDB::~DuckDB() {
}

void DuckDB::Configure(DBConfig &config) {
	if (config.access_mode != AccessMode::UNDEFINED) {
		access_mode = config.access_mode;
	} else {
		access_mode = AccessMode::READ_WRITE;
	}
	if (config.file_system) {
		file_system = move(config.file_system);
	} else {
		file_system = make_unique<FileSystem>();
	}
	checkpoint_only = config.checkpoint_only;
	checkpoint_wal_size = config.checkpoint_wal_size;
	use_direct_io = config.use_direct_io;
}
