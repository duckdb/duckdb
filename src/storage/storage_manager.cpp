#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

StorageManager::StorageManager(AttachedDatabase &db, string path_p, bool read_only)
    : db(db), path(std::move(path_p)), read_only(read_only) {
	if (path.empty()) {
		path = IN_MEMORY_PATH;
	} else {
		auto &fs = FileSystem::Get(db);
		this->path = fs.ExpandPath(path);
	}
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::Get(AttachedDatabase &db) {
	return db.GetStorageManager();
}
StorageManager &StorageManager::Get(Catalog &catalog) {
	return StorageManager::Get(catalog.GetAttached());
}

DatabaseInstance &StorageManager::GetDatabase() {
	return db.GetDatabase();
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
}

const BufferManager &BufferManager::GetBufferManager(const ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
}

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	return context.db->config.options.object_cache_enable;
}

int64_t StorageManager::GetWALSize() {
	if (!wal && !GetWAL()) {
		return 0;
	}
	if (!wal->Initialized()) {
		D_ASSERT(!FileSystem::Get(db).FileExists(GetWALPath()));
		return 0;
	}
	return wal->GetWriter().GetFileSize();
}

optional_ptr<WriteAheadLog> StorageManager::GetWAL() {
	if (InMemory() || read_only || !load_complete) {
		return nullptr;
	}

	if (!wal) {
		auto wal_path = GetWALPath();
		wal = make_uniq<WriteAheadLog>(db, wal_path);

		// If the WAL file exists, then we initialize it.
		if (FileSystem::Get(db).FileExists(wal_path)) {
			wal->Initialize();
		}
	}
	return wal.get();
}

void StorageManager::ResetWAL() {
	auto wal_ptr = GetWAL();
	if (wal_ptr) {
		wal_ptr->Delete();
	}
	wal.reset();
}

string StorageManager::GetWALPath() {

	std::size_t question_mark_pos = path.find('?');
	auto wal_path = path;
	if (question_mark_pos != std::string::npos) {
		wal_path.insert(question_mark_pos, ".wal");
	} else {
		wal_path += ".wal";
	}
	return wal_path;
}

bool StorageManager::InMemory() {
	D_ASSERT(!path.empty());
	return path == IN_MEMORY_PATH;
}

void StorageManager::Initialize() {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

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
	MetadataManager &GetMetadataManager() override {
		return block_manager.GetMetadataManager();
	}
};

SingleFileStorageManager::SingleFileStorageManager(AttachedDatabase &db, string path, bool read_only)
    : StorageManager(db, std::move(path), read_only) {
}

void SingleFileStorageManager::LoadDatabase() {
	if (InMemory()) {
		block_manager = make_uniq<InMemoryBlockManager>(BufferManager::GetBufferManager(db));
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);
		return;
	}

	auto &fs = FileSystem::Get(db);
	auto &config = DBConfig::Get(db);
	if (!config.options.enable_external_access) {
		if (!db.IsInitialDatabase()) {
			throw PermissionException("Attaching on-disk databases is disabled through configuration");
		}
	}

	StorageManagerOptions options;
	options.read_only = read_only;
	options.use_direct_io = config.options.use_direct_io;
	options.debug_initialize = config.options.debug_initialize;

	// first check if the database exists
	if (!read_only && !fs.FileExists(path)) {
		// file does not exist and we are in read-write mode
		// create a new file

		// check if a WAL file already exists
		auto wal_path = GetWALPath();
		if (config.options.kafka_redo_log) {
			wal_path = "kafka://";
			wal_path += config.options.kafka_bootstrap_server_and_port;
			wal_path += "/";
			wal_path += config.options.kafka_topic_name;
			wal_path += "/";
			wal_path += config.options.kafka_writer ? "writer" : "reader";
		}
		// check if the WAL exists - JO/MV - Revisit
		if (!config.options.kafka_redo_log) {
			if (fs.FileExists(wal_path)) {
				// WAL file exists but database file does not
				// remove the WAL
				fs.RemoveFile(wal_path);
			}
		}

		// initialize the block manager while creating a new db file
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->CreateNewDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);
	} else {
		// either the file exists, or we are in read-only mode
		// try to read the existing file on disk

		// initialize the block manager while loading the current db file
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->LoadExistingDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);

		// load the db from storage
		auto checkpoint_reader = SingleFileCheckpointReader(*this);
		checkpoint_reader.LoadFromStorage();

		// check if the WAL file exists
		auto wal_path = GetWALPath();
		auto handle = fs.OpenFile(wal_path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (handle) {
			// replay the WAL
			if (WriteAheadLog::Replay(db, std::move(handle))) {
				fs.RemoveFile(wal_path);
			}
		}
	}

	load_complete = true;
}

///////////////////////////////////////////////////////////////////////////////

class SingleFileStorageCommitState : public StorageCommitState {
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	optional_ptr<WriteAheadLog> log;
	bool checkpoint;

public:
	SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint);
	~SingleFileStorageCommitState() override {
		// If log is non-null, then commit threw an exception before flushing.
		if (log) {
			auto &wal = *log.get();
			wal.skip_writing = false;
			if (wal.GetTotalWritten() > initial_written) {
				// remove any entries written into the WAL by truncating it
				wal.Truncate(NumericCast<int64_t>(initial_wal_size));
			}
		}
	}

	// Make the commit persistent
	void FlushCommit() override;
};

SingleFileStorageCommitState::SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint)
    : checkpoint(checkpoint) {

	log = storage_manager.GetWAL();
	if (!log) {
		return;
	}

	auto initial_size = storage_manager.GetWALSize();
	initial_written = log->GetTotalWritten();
	initial_wal_size = initial_size < 0 ? 0 : idx_t(initial_size);

	if (checkpoint) {
		// True, if we are checkpointing after the current commit.
		// If true, we don't need to write to the WAL, saving unnecessary writes to disk.
		log->skip_writing = true;
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

unique_ptr<StorageCommitState> SingleFileStorageManager::GenStorageCommitState(Transaction &transaction,
                                                                               bool checkpoint) {
	return make_uniq<SingleFileStorageCommitState>(*this, checkpoint);
}

bool SingleFileStorageManager::IsCheckpointClean(MetaBlockPointer checkpoint_id) {
	return block_manager->IsRootBlock(checkpoint_id);
}

void SingleFileStorageManager::CreateCheckpoint(CheckpointOptions options) {
	if (InMemory() || read_only || !load_complete) {
		return;
	}
	if (db.GetStorageExtension()) {
		db.GetStorageExtension()->OnCheckpointStart(db, options);
	}
	auto &config = DBConfig::Get(db);
	if (GetWALSize() > 0 || config.options.force_checkpoint || options.action == CheckpointAction::FORCE_CHECKPOINT) {
		// we only need to checkpoint if there is anything in the WAL
		try {
			SingleFileCheckpointWriter checkpointer(db, *block_manager, options.type);
			checkpointer.CreateCheckpoint();
		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw FatalException("Failed to create checkpoint because of error: %s", error.RawMessage());
		}
	}
	if (options.wal_action == CheckpointWALAction::DELETE_WAL) {
		ResetWAL();
	}

	if (db.GetStorageExtension()) {
		db.GetStorageExtension()->OnCheckpointEnd(db, options);
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
		ds.wal_size = NumericCast<idx_t>(GetWALSize());
	}
	return ds;
}

vector<MetadataBlockInfo> SingleFileStorageManager::GetMetadataInfo() {
	auto &metadata_manager = block_manager->GetMetadataManager();
	return metadata_manager.GetMetadataInfo();
}

bool SingleFileStorageManager::AutomaticCheckpoint(idx_t estimated_wal_bytes) {
	auto initial_size = NumericCast<idx_t>(GetWALSize());
	idx_t expected_wal_size = initial_size + estimated_wal_bytes;
	return expected_wal_size > DBConfig::Get(db).options.checkpoint_wal_size;
}

shared_ptr<TableIOManager> SingleFileStorageManager::GetTableIOManager(BoundCreateTableInfo *info /*info*/) {
	// This is an unmanaged reference. No ref/deref overhead. Lifetime of the
	// TableIoManager follows lifetime of the StorageManager (this).
	return shared_ptr<TableIOManager>(shared_ptr<char>(nullptr), table_io_manager.get());
}

uint64_t SingleFileStorageManager::GetSnapshotId() {
  if (InMemory() || read_only) {
    return 0;
  }
  return dynamic_cast<SingleFileBlockManager *>(block_manager.get())->GetSnapshotId();
}

string SingleFileStorageManager::Snapshot() {
  uint64_t sid = dynamic_cast<SingleFileBlockManager *>(block_manager.get())->GetSnapshotId();
  unique_ptr<FileHandle>& src_handle = dynamic_cast<SingleFileBlockManager *>(block_manager.get())->GetFileHandle();
  string ret = path;
  ret += ".";
  ret += to_string(sid);
  auto &fs = FileSystem::Get(db);
  fs.CopyFile(path, ret, src_handle);
  return ret;
}
 
} // namespace duckdb
