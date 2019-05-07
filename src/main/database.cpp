#include "main/database.hpp"

#include "catalog/catalog.hpp"
#include "common/file_system.hpp"
#include "main/connection_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path, bool read_only) : read_only(read_only) {
	file_system = make_unique<FileSystem>();
	storage = make_unique<StorageManager>(*this, path ? string(path) : string(), read_only);
	catalog = make_unique<Catalog>(*storage);
	transaction_manager = make_unique<TransactionManager>(*storage);
	connection_manager = make_unique<ConnectionManager>();
	// initialize the database
	storage->Initialize();
}

DuckDB::DuckDB(const string &path, bool read_only) : DuckDB(path.c_str(), read_only) {
}

DuckDB::~DuckDB() {
}
