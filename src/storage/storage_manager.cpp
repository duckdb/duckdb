#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/object_cache.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"

namespace duckdb {

StorageManager::StorageManager(DatabaseInstance &db, string path, bool read_only)
    : database(db), path(path), wal(db), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::GetStorageManager(ClientContext &context) {
	return *context.db->storage;
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return *context.db->storage->buffer_manager;
}

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return *context.db->object_cache;
}

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	return context.db->config.object_cache_enable;
}

void StorageManager::Initialize() {
	bool in_memory = path.empty() || path == ":memory:";

	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// first initialize the base system catalogs
	// these are never written to the WAL
	Connection con(database);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	catalog.CreateSchema(*con.context, &info);

	// initialize default functions
	BuiltinFunctions builtin(*con.context, catalog);
	builtin.Initialize();

	// commit transactions
	con.Commit();

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	} else {
		auto &config = DBConfig::GetConfig(*con.context);
		block_manager = make_unique<InMemoryBlockManager>();
		buffer_manager = make_unique<BufferManager>(database.GetFileSystem(), *block_manager,
		                                            config.temporary_directory, config.maximum_memory);
	}
}

void StorageManager::Checkpoint(string wal_path) {
	auto &fs = database.GetFileSystem();
	if (!fs.FileExists(wal_path)) {
		// no WAL to checkpoint
		return;
	}
	if (read_only) {
		// cannot checkpoint in read-only system
		return;
	}
	// check the size of the WAL
	{
		BufferedFileReader reader(fs, wal_path.c_str());
		if (reader.FileSize() <= database.config.checkpoint_wal_size) {
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
	auto &fs = database.GetFileSystem();
	auto &config = database.config;
	// first check if the database exists
	if (!fs.FileExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
		}
		// check if the WAL exists
		if (fs.FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			fs.RemoveFile(wal_path);
		}
		// initialize the block manager while creating a new db file
		block_manager = make_unique<SingleFileBlockManager>(fs, path, read_only, true, config.use_direct_io);
		buffer_manager =
		    make_unique<BufferManager>(fs, *block_manager, config.temporary_directory, config.maximum_memory);
	} else {
		if (!config.checkpoint_only) {
			Checkpoint(wal_path);
		}
		// initialize the block manager while loading the current db file
		auto sf = make_unique<SingleFileBlockManager>(fs, path, read_only, false, config.use_direct_io);
		buffer_manager = make_unique<BufferManager>(fs, *sf, config.temporary_directory, config.maximum_memory);
		sf->LoadFreeList(*buffer_manager);
		block_manager = move(sf);

		//! Load from storage
		CheckpointManager checkpointer(*this);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (fs.FileExists(wal_path)) {
			// replay the WAL
			WriteAheadLog::Replay(database, wal_path);
			if (config.checkpoint_only) {
				D_ASSERT(!read_only);
				// checkpoint the database
				checkpointer.CreateCheckpoint();
				// remove the WAL
				fs.RemoveFile(wal_path);
			}
		}
	}
	// initialize the WAL file
	if (!config.checkpoint_only && !read_only) {
		wal.Initialize(wal_path);
	}
}

} // namespace duckdb
