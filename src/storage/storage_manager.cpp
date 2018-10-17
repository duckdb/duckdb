
#include "catalog/catalog.hpp"

#include "common/exception.hpp"
#include "common/file_system.hpp"

#include "function/function.hpp"

#include "main/database.hpp"

#include "storage/storage_manager.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(DuckDB &database, std::string path)
    : path(path), database(database), wal(database) {}

void StorageManager::Initialize() {
	bool in_memory = path.empty();

	// first initialize the base system catalogs
	// these are never written to the WAL
	auto transaction = database.transaction_manager.StartTransaction();

	// create the default schema
	CreateSchemaInformation info;
	info.schema = DEFAULT_SCHEMA;
	database.catalog.CreateSchema(*transaction, &info);

	// initialize default functions
	BuiltinFunctions::Initialize(*transaction, database.catalog);

	// commit transactions
	database.transaction_manager.CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase(path);
	}
}

void StorageManager::LoadDatabase(std::string &path) {
	// first check if the database exists
	auto wal_path = JoinPath(path, WAL_FILE);
	if (!DirectoryExists(path)) {
		// have to create the directory
		CreateDirectory(path);
	} else {
		// directory already exists
		// verify that it is an existing database
		if (!FileExists(wal_path)) {
			throw IOException(
			    "Database directory exists, but could not find WAL file!");
		}
		// replay the WAL
		wal.Replay(wal_path);
	}
	// initialize the WAL file
	wal.Initialize(wal_path);
}
