#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/in_memory_checkpoint.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "mbedtls_wrapper.hpp"

namespace duckdb {

using SHA256State = duckdb_mbedtls::MbedTlsWrapper::SHA256State;

void StorageOptions::Initialize(const unordered_map<string, Value> &options) {
	string storage_version_user_provided = "";
	for (auto &entry : options) {
		if (entry.first == "block_size") {
			// Extract the block allocation size. This is NOT the actual memory available on a block (block_size),
			// even though the corresponding option we expose to the user is called "block_size".
			block_alloc_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "encryption_key") {
			// check the type of the key
			auto type = entry.second.type();
			if (type.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("\"%s\" is not a valid key. A key must be of type VARCHAR",
				                      entry.second.ToString());
			} else if (entry.second.GetValue<string>().empty()) {
				throw BinderException("Not a valid key. A key cannot be empty");
			}
			user_key = make_shared_ptr<string>(StringValue::Get(entry.second.DefaultCastAs(LogicalType::BLOB)));
			block_header_size = DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE;
			encryption = true;
		} else if (entry.first == "encryption_cipher") {
			auto parsed_cipher = EncryptionTypes::StringToCipher(entry.second.ToString());
			if (parsed_cipher != EncryptionTypes::CipherType::GCM &&
			    parsed_cipher != EncryptionTypes::CipherType::CTR) {
				throw BinderException("\"%s\" is not a valid cipher. Try 'GCM' or 'CTR'.", entry.second.ToString());
			}
			encryption_cipher = parsed_cipher;
		} else if (entry.first == "row_group_size") {
			row_group_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "storage_version") {
			storage_version_user_provided = entry.second.ToString();
			storage_version =
			    SerializationCompatibility::FromString(storage_version_user_provided).serialization_version;
		} else if (entry.first == "compress") {
			if (entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>()) {
				compress_in_memory = CompressInMemory::COMPRESS;
			} else {
				compress_in_memory = CompressInMemory::DO_NOT_COMPRESS;
			}
		} else {
			throw BinderException("Unrecognized option for attach \"%s\"", entry.first);
		}
	}
	if (encryption &&
	    (!storage_version.IsValid() ||
	     storage_version.GetIndex() < SerializationCompatibility::FromString("v1.4.0").serialization_version)) {
		if (!storage_version_user_provided.empty()) {
			throw InvalidInputException(
			    "Explicit provided STORAGE_VERSION (\"%s\") and ENCRYPTION_KEY (storage >= v1.4.0) are not compatible",
			    storage_version_user_provided);
		}
		// set storage version to v1.4.0
		storage_version = SerializationCompatibility::FromString("v1.4.0").serialization_version;
	}
}

StorageManager::StorageManager(AttachedDatabase &db, string path_p, const AttachOptions &options)
    : db(db), path(std::move(path_p)), read_only(options.access_mode == AccessMode::READ_ONLY),
      in_memory_change_size(0) {
	if (path.empty()) {
		path = IN_MEMORY_PATH;
		return;
	}
	auto &fs = FileSystem::Get(db);
	path = fs.ExpandPath(path);

	storage_options.Initialize(options.options);
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

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

idx_t StorageManager::GetWALSize() {
	if (InMemory() || wal->GetDatabase().GetRecoveryMode() == RecoveryMode::NO_WAL_WRITES) {
		return in_memory_change_size.load();
	}
	return wal->GetWALSize();
}

optional_ptr<WriteAheadLog> StorageManager::GetWAL() {
	if (InMemory() || read_only || !load_complete) {
		return nullptr;
	}
	return wal.get();
}

void StorageManager::ResetWAL() {
	wal->Delete();
}

string StorageManager::GetWALPath() const {
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

bool StorageManager::InMemory() const {
	D_ASSERT(!path.empty());
	return path == IN_MEMORY_PATH;
}

void StorageManager::Destroy() {
}

inline void ClearUserKey(shared_ptr<string> const &encryption_key) {
	if (encryption_key && !encryption_key->empty()) {
		duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(data_ptr_cast(&(*encryption_key)[0]),
		                                                                 encryption_key->size());
		encryption_key->clear();
	}
}

void StorageManager::Initialize(QueryContext context) {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// Create or load the database from disk, if not in-memory mode.
	LoadDatabase(context);

	if (storage_options.encryption) {
		ClearUserKey(storage_options.user_key);
	}
}

class SingleFileTableIOManager : public TableIOManager {
public:
	explicit SingleFileTableIOManager(BlockManager &block_manager, idx_t row_group_size)
	    : block_manager(block_manager), row_group_size(row_group_size) {
	}

	BlockManager &block_manager;
	idx_t row_group_size;

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
	idx_t GetRowGroupSize() const override {
		return row_group_size;
	}
};

SingleFileStorageManager::SingleFileStorageManager(AttachedDatabase &db, string path, const AttachOptions &options)
    : StorageManager(db, std::move(path), options) {
}

void SingleFileStorageManager::LoadDatabase(QueryContext context) {
	if (InMemory()) {
		block_manager = make_uniq<InMemoryBlockManager>(BufferManager::GetBufferManager(db), DEFAULT_BLOCK_ALLOC_SIZE,
		                                                DEFAULT_BLOCK_HEADER_STORAGE_SIZE);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager, DEFAULT_ROW_GROUP_SIZE);
		// in-memory databases can always use the latest storage version
		storage_version = GetSerializationVersion("latest");
		load_complete = true;
		return;
	}
	if (storage_options.compress_in_memory != CompressInMemory::AUTOMATIC) {
		throw InvalidInputException("COMPRESS can only be set for in-memory databases");
	}

	auto &fs = FileSystem::Get(db);
	auto &config = DBConfig::Get(db);

	StorageManagerOptions options;
	options.read_only = read_only;
	options.use_direct_io = config.options.use_direct_io;
	options.debug_initialize = config.options.debug_initialize;
	options.storage_version = storage_options.storage_version;

	if (storage_options.encryption) {
		// key is given upon ATTACH
		D_ASSERT(storage_options.block_header_size == DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE);
		options.encryption_options.encryption_enabled = true;
		options.encryption_options.user_key = std::move(storage_options.user_key);
	}

	idx_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
	if (storage_options.row_group_size.IsValid()) {
		row_group_size = storage_options.row_group_size.GetIndex();
		if (row_group_size == 0) {
			throw NotImplementedException("Invalid row group size: %llu - row group size must be bigger than 0",
			                              row_group_size);
		}
		if (row_group_size % STANDARD_VECTOR_SIZE != 0) {
			throw NotImplementedException(
			    "Invalid row group size: %llu - row group size must be divisible by the vector size (%llu)",
			    row_group_size, STANDARD_VECTOR_SIZE);
		}
	}
	// Check if the database file already exists.
	// Note: a file can also exist if there was a ROLLBACK on a previous transaction creating that file.
	if (!read_only && !fs.FileExists(path)) {
		// file does not exist and we are in read-write mode
		// create a new file

		auto wal_path = GetWALPath();
		// try to remove the WAL file if it exists
		fs.TryRemoveFile(wal_path);

		// Set the block allocation size for the new database file.
		if (storage_options.block_alloc_size.IsValid()) {
			// Use the option provided by the user.
			Storage::VerifyBlockAllocSize(storage_options.block_alloc_size.GetIndex());
			options.block_alloc_size = storage_options.block_alloc_size;
		} else {
			// No explicit option provided: use the default option.
			options.block_alloc_size = config.options.default_block_alloc_size;
		}
		//! set the block header size for the encrypted database files
		//! set the database to encrypted
		//! update the storage version to 1.4.0
		if (storage_options.block_header_size.IsValid()) {
			// Use the header size for the corresponding encryption algorithm.
			Storage::VerifyBlockHeaderSize(storage_options.block_header_size.GetIndex());
			options.block_header_size = storage_options.block_header_size;
			options.storage_version = storage_options.storage_version;
		} else {
			// No encryption; use the default option.
			options.block_header_size = config.options.default_block_header_size;
		}
		if (!options.storage_version.IsValid()) {
			// when creating a new database we default to the serialization version specified in the config
			options.storage_version = config.options.serialization_compatibility.serialization_version;
		}

		// Initialize the block manager before creating a new database.
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->CreateNewDatabase(context);
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager, row_group_size);
		wal = make_uniq<WriteAheadLog>(db, wal_path);

	} else {
		// Either the file exists, or we are in read-only mode, so we
		// try to read the existing file on disk.

		// set the block header size for the encrypted database files
		// (also if they already exist)
		if (storage_options.encryption) {
			options.encryption_options.encryption_enabled = true;
			D_ASSERT(storage_options.block_header_size == DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE);
		}
		if (storage_options.block_header_size.IsValid()) {
			Storage::VerifyBlockHeaderSize(storage_options.block_header_size.GetIndex());
			options.block_header_size = storage_options.block_header_size;
			options.storage_version = storage_options.storage_version;
		} else {
			// No explicit option provided: use the default option.
			options.block_header_size = config.options.default_block_header_size;
		}

		// Initialize the block manager while loading the database file.
		// We'll construct the SingleFileBlockManager with the default block allocation size,
		// and later adjust it when reading the file header.
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->LoadExistingDatabase(context);
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager, row_group_size);

		if (storage_options.block_alloc_size.IsValid()) {
			// user-provided block alloc size
			idx_t block_alloc_size = storage_options.block_alloc_size.GetIndex();
			if (block_alloc_size != block_manager->GetBlockAllocSize()) {
				throw InvalidInputException(
				    "block size parameter does not match the file's block size, got %llu, expected %llu",
				    storage_options.block_alloc_size.GetIndex(), block_manager->GetBlockAllocSize());
			}
		}

		if (storage_options.block_header_size.IsValid()) {
			// block header size for encrypted database files
			idx_t block_header_size = storage_options.block_header_size.GetIndex();
			if (block_header_size != block_manager->GetBlockHeaderSize()) {
				throw InvalidInputException(
				    "block header size does not match the file's block header size, got %llu, expected %llu",
				    storage_options.block_header_size.GetIndex(), block_manager->GetBlockHeaderSize());
			}
		}

		// Start timing the storage load step.
		auto client_context = context.GetClientContext();
		if (client_context) {
			auto profiler = client_context->client_data->profiler;
			profiler->StartTimer(MetricsType::ATTACH_LOAD_STORAGE_LATENCY);
		}

		// Load the checkpoint from storage.
		auto checkpoint_reader = SingleFileCheckpointReader(*this);
		checkpoint_reader.LoadFromStorage();

		// End timing the storage load step.
		if (client_context) {
			auto profiler = client_context->client_data->profiler;
			profiler->EndTimer(MetricsType::ATTACH_LOAD_STORAGE_LATENCY);
		}

		// Start timing the WAL replay step.
		if (client_context) {
			auto profiler = client_context->client_data->profiler;
			profiler->StartTimer(MetricsType::ATTACH_REPLAY_WAL_LATENCY);
		}

		// Replay the WAL.
		auto wal_path = GetWALPath();
		wal = WriteAheadLog::Replay(context, fs, db, wal_path);

		// End timing the WAL replay step.
		if (client_context) {
			auto profiler = client_context->client_data->profiler;
			profiler->EndTimer(MetricsType::ATTACH_REPLAY_WAL_LATENCY);
		}
	}

	if (row_group_size > 122880ULL && GetStorageVersion() < 4) {
		throw InvalidInputException("Unsupported row group size %llu - row group sizes >= 122_880 are only supported "
		                            "with STORAGE_VERSION '1.2.0' or above.\nExplicitly specify a newer storage "
		                            "version when creating the database to enable larger row groups",
		                            row_group_size);
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
		// Truncate the WAL in case of a destructor.
		RevertCommit();
	} catch (std::exception &ex) {
		ErrorData data(ex);
		try {
			DUCKDB_LOG_ERROR(wal.GetDatabase().GetDatabase(),
			                 "SingleFileStorageCommitState::~SingleFileStorageCommitState()\t\t" + data.Message());
		} catch (...) { // NOLINT
		}
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

unique_ptr<CheckpointWriter> SingleFileStorageManager::CreateCheckpointWriter(QueryContext context,
                                                                              CheckpointOptions options) {
	if (InMemory()) {
		return make_uniq<InMemoryCheckpointer>(context, db, *block_manager, *this, options.type);
	}
	return make_uniq<SingleFileCheckpointWriter>(context, db, *block_manager, options.type);
}

void SingleFileStorageManager::CreateCheckpoint(QueryContext context, CheckpointOptions options) {
	if (read_only || !load_complete) {
		return;
	}
	if (db.GetStorageExtension()) {
		db.GetStorageExtension()->OnCheckpointStart(db, options);
	}

	auto &config = DBConfig::Get(db);
	// We only need to checkpoint if there is anything in the WAL.
	auto wal_size = GetWALSize();
	if (wal_size > 0 || config.options.force_checkpoint || options.action == CheckpointAction::ALWAYS_CHECKPOINT) {
		try {
			// Start timing the checkpoint.
			auto client_context = context.GetClientContext();
			if (client_context) {
				auto profiler = client_context->client_data->profiler;
				profiler->StartTimer(MetricsType::CHECKPOINT_LATENCY);
			}

			// Write the checkpoint.
			auto checkpointer = CreateCheckpointWriter(context, options);
			checkpointer->CreateCheckpoint();

			// End timing the checkpoint.
			if (client_context) {
				auto profiler = client_context->client_data->profiler;
				profiler->EndTimer(MetricsType::CHECKPOINT_LATENCY);
			}

		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw FatalException("Failed to create checkpoint because of error: %s", error.Message());
		}
	}

	if (!InMemory() && options.wal_action == CheckpointWALAction::DELETE_WAL) {
		ResetWAL();
	}

	if (db.GetStorageExtension()) {
		db.GetStorageExtension()->OnCheckpointEnd(db, options);
	}
}

void SingleFileStorageManager::Destroy() {
	if (!load_complete) {
		return;
	}
	vector<reference<SchemaCatalogEntry>> schemas;
	// we scan the set of committed schemas
	auto &catalog = Catalog::GetCatalog(db).Cast<DuckCatalog>();
	catalog.ScanSchemas([&](SchemaCatalogEntry &entry) { schemas.push_back(entry); });

	vector<reference<DuckTableEntry>> tables;
	for (auto &schema : schemas) {
		schema.get().Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry.Cast<DuckTableEntry>());
			}
		});
	}

	for (auto &table : tables) {
		auto &data_table = table.get().GetStorage();
		data_table.Destroy();
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
