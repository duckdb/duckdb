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
#include "common/serializer/buffered_file_reader.hpp"

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
	BuiltinFunctions builtin(*transaction, *database.catalog);
	builtin.Initialize();

	// commit transactions
	database.transaction_manager->CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	}
}

void StorageManager::Checkpoint(string wal_path) {
	if (!database.file_system->FileExists(wal_path)) {
		// no WAL to checkpoint
		return;
	}
	if (read_only) {
		// cannot checkpoint in read-only system
		return;
	}
	// check the size of the WAL
	{
		BufferedFileReader reader(*database.file_system, wal_path.c_str());
		if (reader.FileSize() <= database.checkpoint_wal_size) {
			// WAL is too small
			return;
		}
	}

	// checkpoint the database
	// FIXME: we do this now by creating a new database and forcing a checkpoint in that database
	// then reloading the file again
	// this should be fixed and turned into an incremental checkpoint
	DBConfig config;
	config.checkpoint_only = true;
	DuckDB db(path, &config);
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
		block_manager =
		    make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, true, database.use_direct_io);
	} else {
		if (!database.checkpoint_only) {
			Checkpoint(wal_path);
		}
		// initialize the block manager while loading the current db file
		block_manager =
		    make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, false, database.use_direct_io);
		//! Load from storage
		CheckpointManager checkpointer(*this);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (database.file_system->FileExists(wal_path)) {
			// replay the WAL
			WriteAheadLog::Replay(database, wal_path);
			if (database.checkpoint_only) {
				assert(!read_only);
				// checkpoint the database
				checkpointer.CreateCheckpoint();
				// remove the WAL
				database.file_system->RemoveFile(wal_path);
			}
		}
	}
	// initialize the WAL file
	if (!database.checkpoint_only && !read_only) {
		wal.Initialize(wal_path);
	}
}
