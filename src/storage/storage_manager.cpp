
#include "catalog/catalog.hpp"

#include "common/exception.hpp"
#include "common/file_system.hpp"

#include "storage/storage_manager.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(std::string path) : path(path), wal() {}

void StorageManager::Initialize(TransactionManager &transaction_manager,
                                Catalog &catalog) {
	bool in_memory = path.empty();

	// first initialize the base system catalogs
	// these are never written to the WAL
	auto transaction = transaction_manager.StartTransaction();
	catalog.CreateSchema(*transaction, DEFAULT_SCHEMA);
	transaction_manager.CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase(transaction_manager, catalog, path);
	}
}

void StorageManager::LoadDatabase(TransactionManager &transaction_manager,
                                  Catalog &catalog, std::string &path) {
	// first check if the database exists
	auto wal_path = JoinPath(path, WAL_FILE);
	if (!DirectoryExists(path)) {
		// have to create the directory
		CreateDirectory(path);
	} else {
		// directory already exists
		// verify that it is an existing database
		if (!FileExists(wal_path)) {
			throw Exception(
			    "Database directory exists, but could not find WAL file!");
		}
		// replay the WAL
		wal.Replay(transaction_manager, catalog, wal_path);
	}
	// initialize the WAL file
	wal.Initialize(wal_path);
}
