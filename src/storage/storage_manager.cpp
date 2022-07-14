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
    : db(db), path(move(path)), wal(db), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::GetStorageManager(ClientContext &context) {
	return StorageManager::GetStorageManager(*context.db);
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
}

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	return context.db->config.options.object_cache_enable;
}

bool StorageManager::InMemory() {
	return path.empty() || path == ":memory:";
}

void StorageManager::Initialize() {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}
	auto &config = DBConfig::GetConfig(db);
	auto &catalog = Catalog::GetCatalog(db);
	buffer_manager = make_unique<BufferManager>(db, config.options.temporary_directory, config.options.maximum_memory);

	// first initialize the base system catalogs
	// these are never written to the WAL
	Connection con(db);
	con.BeginTransaction();

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	catalog.CreateSchema(*con.context, &info);

	if (config.options.initialize_default_database) {
		// initialize default functions
		BuiltinFunctions builtin(*con.context, catalog);
		builtin.Initialize();
	}

	// commit transactions
	con.Commit();

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	} else {
		block_manager = make_unique<InMemoryBlockManager>();
	}
}

void StorageManager::LoadDatabase() {
	string wal_path = path + ".wal";
	auto &fs = db.GetFileSystem();
	auto &config = db.config;
	bool truncate_wal = false;
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
		block_manager = make_unique<SingleFileBlockManager>(db, path, read_only, true, config.options.use_direct_io);
	} else {
		// initialize the block manager while loading the current db file
		auto sf_bm = make_unique<SingleFileBlockManager>(db, path, read_only, false, config.options.use_direct_io);
		auto sf = sf_bm.get();
		block_manager = move(sf_bm);
		sf->LoadFreeList();

		//! Load from storage
		CheckpointManager checkpointer(db);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (fs.FileExists(wal_path)) {
			// replay the WAL
			truncate_wal = WriteAheadLog::Replay(db, wal_path);
		}
	}
	// initialize the WAL file
	if (!read_only) {
		wal.Initialize(wal_path);
		if (truncate_wal) {
			wal.Truncate(0);
		}
	}
}

void StorageManager::CreateCheckpoint(bool delete_wal, bool force_checkpoint) {
	if (InMemory() || read_only || !wal.initialized) {
		return;
	}
	if (wal.GetWALSize() > 0 || db.config.options.force_checkpoint || force_checkpoint) {
		// we only need to checkpoint if there is anything in the WAL
		CheckpointManager checkpointer(db);
		checkpointer.CreateCheckpoint();
	}
	if (delete_wal) {
		wal.Delete();
	}
}

} // namespace duckdb
