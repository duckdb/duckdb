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
    : db(db), path(move(path)), read_only(read_only) {
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

void StorageManager::CreateBufferManager() {
	auto &config = DBConfig::GetConfig(db);
	buffer_manager = make_unique<BufferManager>(db, config.options.temporary_directory, config.options.maximum_memory);
}

void StorageManager::Initialize() {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}
	CreateBufferManager();

	auto &config = DBConfig::GetConfig(db);
	auto &catalog = Catalog::GetCatalog(db);

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

	// create or load the database from disk, if not in-memory mode
	LoadDatabase();
}

///////////////////////////////////////////////////////////////////////////
class SingleFileTableIOManager : public TableIOManager {
public:
	explicit SingleFileTableIOManager(BlockManager &block_manager) : block_manager(block_manager) {
	}

	BlockManager &block_manager;

public:
	BlockManager &GetIndexBlockManager() override {
		return block_manager;
	}
	BlockManager &GetBlockManagerForRowData() override {
		return block_manager;
	}
};

SingleFileStorageManager::SingleFileStorageManager(DatabaseInstance &db, string path, bool read_only)
    : StorageManager(db, move(path), read_only) {
}

void SingleFileStorageManager::LoadDatabase() {
	if (InMemory()) {
		block_manager = make_unique<InMemoryBlockManager>(*buffer_manager);
		table_io_manager = make_unique<SingleFileTableIOManager>(*block_manager);
		return;
	}

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
		table_io_manager = make_unique<SingleFileTableIOManager>(*block_manager);
	} else {
		// initialize the block manager while loading the current db file
		block_manager = make_unique<SingleFileBlockManager>(db, path, read_only, false, config.options.use_direct_io);
		table_io_manager = make_unique<SingleFileTableIOManager>(*block_manager);

		//! Load from storage
		auto checkpointer = SingleFileCheckpointReader(*this);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (fs.FileExists(wal_path)) {
			// replay the WAL
			truncate_wal = WriteAheadLog::Replay(db, wal_path);
		}
	}
	// initialize the WAL file
	if (!read_only) {
		wal = make_unique<WriteAheadLog>(db, wal_path);
		if (truncate_wal) {
			wal->Truncate(0);
		}
	}
}

///////////////////////////////////////////////////////////////////////////////

class SingleFileStorageCommitState : public StorageCommitState {
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	WriteAheadLog *log;
	bool checkpoint;

public:
	SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint);
	~SingleFileStorageCommitState() override;

	// Make the commit persistent
	void FlushCommit() override;
};

SingleFileStorageCommitState::SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint)
    : checkpoint(checkpoint) {
	log = storage_manager.GetWriteAheadLog();
	if (log) {
		auto initial_size = log->GetWALSize();
		initial_written = log->GetTotalWritten();
		initial_wal_size = initial_size < 0 ? 0 : idx_t(initial_size);

		if (checkpoint) {
			// check if we are checkpointing after this commit
			// if we are checkpointing, we don't need to write anything to the WAL
			// this saves us a lot of unnecessary writes to disk in the case of large commits
			log->skip_writing = true;
		}
	} else {
		D_ASSERT(!checkpoint);
	}
}

// Make the commit persistent
void SingleFileStorageCommitState::FlushCommit() {
	if (log) {
		// flush the WAL if any changes were made
		if (log->GetTotalWritten() > initial_written) {
			(void)checkpoint;
			D_ASSERT(!checkpoint);
			D_ASSERT(!log->skip_writing);
			log->Flush();
		}
		log->skip_writing = false;
	}
	// Null so that the destructor will not truncate the log.
	log = nullptr;
}

SingleFileStorageCommitState::~SingleFileStorageCommitState() {
	// If log is non-null, then commit threw an exception before flushing.
	if (log) {
		log->skip_writing = false;
		if (log->GetTotalWritten() > initial_written) {
			// remove any entries written into the WAL by truncating it
			log->Truncate(initial_wal_size);
		}
	}
}

unique_ptr<StorageCommitState> SingleFileStorageManager::GenStorageCommitState(Transaction &transaction,
                                                                               bool checkpoint) {
	return make_unique<SingleFileStorageCommitState>(*this, checkpoint);
}

bool SingleFileStorageManager::IsCheckpointClean(block_id_t checkpoint_id) {
	return block_manager->IsRootBlock(checkpoint_id);
}

void SingleFileStorageManager::CreateCheckpoint(bool delete_wal, bool force_checkpoint) {
	if (InMemory() || read_only || !wal) {
		return;
	}
	if (wal->GetWALSize() > 0 || db.config.options.force_checkpoint || force_checkpoint) {
		// we only need to checkpoint if there is anything in the WAL
		SingleFileCheckpointWriter checkpointer(db, *block_manager);
		checkpointer.CreateCheckpoint();
	}
	if (delete_wal) {
		wal->Delete();
		wal.reset();
	}
}

DatabaseSize SingleFileStorageManager::GetDatabaseSize() {
	// All members default to zero
	DatabaseSize ds;
	if (!InMemory()) {
		ds.total_blocks = block_manager->TotalBlocks();
		ds.block_size = Storage::BLOCK_ALLOC_SIZE;
		ds.free_blocks = block_manager->FreeBlocks();
		ds.used_blocks = ds.total_blocks - ds.free_blocks;
		ds.bytes = (ds.total_blocks * ds.block_size);
		if (auto wal = GetWriteAheadLog()) {
			ds.wal_size = wal->GetWALSize();
		}
	}
	return ds;
}

bool SingleFileStorageManager::AutomaticCheckpoint(idx_t estimated_wal_bytes) {
	auto log = GetWriteAheadLog();
	if (!log) {
		return false;
	}

	auto initial_size = log->GetWALSize();
	idx_t expected_wal_size = initial_size + estimated_wal_bytes;
	return expected_wal_size > db.config.options.checkpoint_wal_size;
}

shared_ptr<TableIOManager> SingleFileStorageManager::GetTableIOManager(BoundCreateTableInfo *info /*info*/) {
	// This is an unmanaged reference. No ref/deref overhead. Lifetime of the
	// TableIoManager follows lifetime of the StorageManager (this).
	return shared_ptr<TableIOManager>(shared_ptr<char>(nullptr), table_io_manager.get());
}

} // namespace duckdb
