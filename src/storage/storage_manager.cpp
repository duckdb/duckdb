#include "storage/storage_manager.hpp"
#include "storage/checkpoint_manager.hpp"
#include "storage/single_file_block_manager.hpp"

#include "catalog/catalog.hpp"
#include "common/file_system.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "transaction/transaction_manager.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(DuckDB &db, string path, bool read_only)
    : database(db), path(path), wal(db), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

void StorageManager::Initialize() {
	bool in_memory = path.empty() || path == ":memory:";

	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// first initialize the base system catalogs
	// these are never written to the WAL
	auto transaction = database.transaction_manager->StartTransaction();

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	database.catalog->CreateSchema(*transaction, &info);

	// initialize default functions
	BuiltinFunctions::Initialize(*transaction, *database.catalog);

	// commit transactions
	database.transaction_manager->CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	}
}

void StorageManager::LoadDatabase() {
	string wal_path = path + ".wal";
	// first check if the database exists
	if (!database.file_system->FileExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist",
			                       path.c_str());
		}
		// check if the WAL exists
		if (database.file_system->FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			database.file_system->RemoveFile(wal_path);
		}
		// initialize the block manager while creating a new db file
		block_manager = make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, true);
	} else {
		// initialize the block manager while loading the current db file
		block_manager = make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, false);
		//! Load from storage
		CheckpointManager checkpointer(*this);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (database.file_system->FileExists(wal_path)) {
			// replay the WAL
			WriteAheadLog::Replay(database, wal_path);
			if (!read_only) {
				// checkpoint the database
				checkpointer.CreateCheckpoint();
				// remove the WAL
				database.file_system->RemoveFile(wal_path);
			}
		}
	}
	// FIXME: check if temporary file exists and delete that if it does
	// initialize the WAL file
	if (!read_only) {
		wal.Initialize(wal_path);
	}
}
