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
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

StorageManager::StorageManager(AttachedDatabase &db, string path_p, bool read_only)
    : db(db), path(std::move(path_p)), read_only(read_only) {

	if (path.empty()) {
		path = IN_MEMORY_PATH;
		return;
	}
	auto &fs = FileSystem::Get(db);
	path = fs.ExpandPath(path);
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

idx_t StorageManager::GetWALSize() {
	auto wal_ptr = GetWAL();
	if (!wal_ptr) {
		return 0;
	}
	return wal_ptr->GetWALSize();
}

optional_ptr<WriteAheadLog> StorageManager::GetWAL() {
	if (InMemory() || read_only || !load_complete) {
		return nullptr;
	}

	if (!wal) {
		auto wal_path = GetWALPath();
		wal = make_uniq<WriteAheadLog>(db, wal_path);
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
	// we append the ".wal" **before** a question mark in case of GET parameters
	// but only if we are not in a windows long path (which starts with \\?\)
	std::size_t question_mark_pos = std::string::npos;
	if (!StringUtil::StartsWith(path, "\\\\?\\")) {
		question_mark_pos = path.find('?');
	}
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

void StorageManager::Initialize(const optional_idx block_alloc_size) {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// Create or load the database from disk, if not in-memory mode.
	LoadDatabase(block_alloc_size);
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

void SingleFileStorageManager::LoadDatabase(const optional_idx block_alloc_size) {
	if (InMemory()) {
		block_manager = make_uniq<InMemoryBlockManager>(BufferManager::GetBufferManager(db), DEFAULT_BLOCK_ALLOC_SIZE);
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

	// Check if the database file already exists.
	// Note: a file can also exist if there was a ROLLBACK on a previous transaction creating that file.
	if (!read_only && !fs.FileExists(path)) {
		// file does not exist and we are in read-write mode
		// create a new file

		// check if a WAL file already exists
		auto wal_path = GetWALPath();
		if (fs.FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			fs.RemoveFile(wal_path);
		}

		// Set the block allocation size for the new database file.
		if (block_alloc_size.IsValid()) {
			// Use the option provided by the user.
			options.block_alloc_size = block_alloc_size;
		} else {
			// No explicit option provided: use the default option.
			options.block_alloc_size = config.options.default_block_alloc_size;
		}

		// Initialize the block manager before creating a new database.
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->CreateNewDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);

	} else {
		// Either the file exists, or we are in read-only mode, so we
		// try to read the existing file on disk.

		// Initialize the block manager while loading the database file.
		// We'll construct the SingleFileBlockManager with the default block allocation size,
		// and later adjust it when reading the file header.
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->LoadExistingDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);

		if (block_alloc_size.IsValid() && block_alloc_size.GetIndex() != block_manager->GetBlockAllocSize()) {
			throw InvalidInputException(
			    "block size parameter does not match the file's block size, got %llu, expected %llu",
			    block_alloc_size.GetIndex(), block_manager->GetBlockAllocSize());
		}

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

enum class WALCommitState { IN_PROGRESS, FLUSHED, TRUNCATED };

struct OptimisticallyWrittenRowGroupData {
	OptimisticallyWrittenRowGroupData(idx_t start, idx_t count, unique_ptr<PersistentCollectionData> row_group_data_p)
	    : start(start), count(count), row_group_data(std::move(row_group_data_p)) {
	}

	idx_t start;
	idx_t count;
	unique_ptr<PersistentCollectionData> row_group_data;
};

class SingleFileStorageCommitState : public StorageCommitState {
public:
	SingleFileStorageCommitState(StorageManager &storage, WriteAheadLog &log);
	~SingleFileStorageCommitState() override;

	//! Revert the commit
	void RevertCommit() override;
	// Make the commit persistent
	void FlushCommit() override;

	void AddRowGroupData(DataTable &table, idx_t start_index, idx_t count,
	                     unique_ptr<PersistentCollectionData> row_group_data) override;
	optional_ptr<PersistentCollectionData> GetRowGroupData(DataTable &table, idx_t start_index, idx_t &count) override;
	bool HasRowGroupData() override;

private:
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	WriteAheadLog &wal;
	WALCommitState state;
	reference_map_t<DataTable, unordered_map<idx_t, OptimisticallyWrittenRowGroupData>> optimistically_written_data;
};

SingleFileStorageCommitState::SingleFileStorageCommitState(StorageManager &storage, WriteAheadLog &wal)
    : wal(wal), state(WALCommitState::IN_PROGRESS) {
	auto initial_size = storage.GetWALSize();
	initial_written = wal.GetTotalWritten();
	initial_wal_size = initial_size;
}

SingleFileStorageCommitState::~SingleFileStorageCommitState() {
	if (state != WALCommitState::IN_PROGRESS) {
		return;
	}
	try {
		// truncate the WAL in case of a destructor
		RevertCommit();
	} catch (...) { // NOLINT
	}
}

void SingleFileStorageCommitState::RevertCommit() {
	if (state != WALCommitState::IN_PROGRESS) {
		return;
	}
	if (wal.GetTotalWritten() > initial_written) {
		// remove any entries written into the WAL by truncating it
		wal.Truncate(initial_wal_size);
	}
	state = WALCommitState::TRUNCATED;
}

void SingleFileStorageCommitState::FlushCommit() {
	if (state != WALCommitState::IN_PROGRESS) {
		return;
	}
	wal.Flush();
	state = WALCommitState::FLUSHED;
}

void SingleFileStorageCommitState::AddRowGroupData(DataTable &table, idx_t start_index, idx_t count,
                                                   unique_ptr<PersistentCollectionData> row_group_data) {
	if (row_group_data->HasUpdates()) {
		// cannot serialize optimistic block pointers if in-memory updates exist
		return;
	}
	if (table.HasIndexes()) {
		// cannot serialize optimistic block pointers if the table has indexes
		return;
	}
	auto &entries = optimistically_written_data[table];
	auto entry = entries.find(start_index);
	if (entry != entries.end()) {
		throw InternalException("FIXME: AddOptimisticallyWrittenRowGroup is writing a duplicate row group");
	}
	entries.insert(
	    make_pair(start_index, OptimisticallyWrittenRowGroupData(start_index, count, std::move(row_group_data))));
}

optional_ptr<PersistentCollectionData> SingleFileStorageCommitState::GetRowGroupData(DataTable &table,
                                                                                     idx_t start_index, idx_t &count) {
	auto entry = optimistically_written_data.find(table);
	if (entry == optimistically_written_data.end()) {
		// no data for this table
		return nullptr;
	}
	auto &row_groups = entry->second;
	auto start_entry = row_groups.find(start_index);
	if (start_entry == row_groups.end()) {
		// this row group was not optimistically written
		return nullptr;
	}
	count = start_entry->second.count;
	return start_entry->second.row_group_data.get();
}

bool SingleFileStorageCommitState::HasRowGroupData() {
	return !optimistically_written_data.empty();
}

unique_ptr<StorageCommitState> SingleFileStorageManager::GenStorageCommitState(WriteAheadLog &wal) {
	return make_uniq<SingleFileStorageCommitState>(*this, wal);
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
	if (GetWALSize() > 0 || config.options.force_checkpoint || options.action == CheckpointAction::ALWAYS_CHECKPOINT) {
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
		ds.block_size = block_manager->GetBlockAllocSize();
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

BlockManager &SingleFileStorageManager::GetBlockManager() {
	return *block_manager;
}

} // namespace duckdb
