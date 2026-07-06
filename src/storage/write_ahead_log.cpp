#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/error_data.hpp"

#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/wal_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

constexpr uint64_t WAL_VERSION_NUMBER = 2;
constexpr uint64_t WAL_ENCRYPTED_VERSION_NUMBER = 3;

WriteAheadLog::WriteAheadLog(StorageManager &storage_manager, const string &wal_path, idx_t wal_size,
                             WALInitState init_state, optional_idx checkpoint_iteration)
    : storage_manager(storage_manager), wal_path(wal_path), init_state(init_state),
      checkpoint_iteration(checkpoint_iteration) {
	storage_manager.SetWALSize(wal_size);
	storage_manager.ResetWALEntriesCount();
}

WriteAheadLog::~WriteAheadLog() {
}

AttachedDatabase &WriteAheadLog::GetDatabase() {
	return storage_manager.GetAttached();
}

StorageManager &WriteAheadLog::GetStorageManager() {
	return storage_manager;
}

BufferedFileWriter &WriteAheadLog::Initialize() {
	if (Initialized()) {
		return *writer;
	}
	lock_guard<mutex> lock(wal_lock);
	if (!writer) {
		writer =
		    make_uniq<BufferedFileWriter>(FileSystem::Get(GetDatabase()), wal_path,
		                                  FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                      FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS);
		if (init_state == WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE) {
			writer->Truncate(storage_manager.GetWALSize());
		} else {
			storage_manager.SetWALSize(writer->GetFileSize());
		}
		sync_lane_cap = writer->handle->file_system.SyncParallelism(*writer->handle) == FileSyncParallelism::PARALLEL
		                    ? NumericLimits<idx_t>::Maximum()
		                    : 1;
		init_state = WALInitState::INITIALIZED;
	}
	return *writer;
}

idx_t WriteAheadLog::GetTotalWritten() const {
	if (!Initialized()) {
		return 0;
	}
	return writer->GetTotalWritten();
}

void WriteAheadLog::Truncate(idx_t size) {
	if (init_state == WALInitState::NO_WAL) {
		// no WAL to truncate
		return;
	}
	if (!Initialized()) {
		init_state = WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE;
		storage_manager.SetWALSize(size);
		return;
	}
	writer->Truncate(size);
	idx_t truncated = writer->GetFileSize();
	storage_manager.SetWALSize(truncated);
	// Truncate runs under the WAL lock (the same lock that serializes the FlushAppendNoSync publish), so lower the
	// published page-cache offset too -- otherwise a later fsync could snapshot a target beyond the file. The order
	// below is load-bearing: after the flushed clamp, the generation bump makes the clamped offset visible to every
	// fsync that starts afterwards (its acquire-load of the generation precedes its target snapshot); an fsync from
	// before the bump discards its durable_offset raise on the generation mismatch; and the drain guarantees no raise
	// can land after the final durable clamp.
	if (truncated < flushed_offset.load(std::memory_order_acquire)) {
		flushed_offset.store(truncated, std::memory_order_release);
	}
	truncate_gen.fetch_add(1, std::memory_order_acq_rel);
	while (active_syncs.load(std::memory_order_acquire) > 0) {
		uint64_t epoch = sync_epoch.load(std::memory_order_acquire);
		if (active_syncs.load(std::memory_order_acquire) == 0) {
			break;
		}
		WaitSyncEpochChange(epoch);
	}
	if (truncated < durable_offset.load(std::memory_order_acquire)) {
		durable_offset.store(truncated, std::memory_order_release);
	}
}

bool WriteAheadLog::Initialized() const {
	return init_state == WALInitState::INITIALIZED;
}

//===--------------------------------------------------------------------===//
// Serializer
//===--------------------------------------------------------------------===//
class ChecksumWriter : public WriteStream {
public:
	explicit ChecksumWriter(WriteAheadLog &wal) : wal(wal), memory_stream(Allocator::Get(wal.GetDatabase())) {
	}

	void WriteData(const_data_ptr_t buffer, idx_t write_size) override {
		// buffer data into the memory stream
		memory_stream.WriteData(buffer, write_size);
	}

	void Flush() {
		if (!stream) {
			stream = wal.Initialize();
		}

		// if the config.encrypt WAL is true
		// and if the attached database is encrypted
		// then encrypt WAL before flushing
		auto &catalog = wal.GetDatabase().GetCatalog().Cast<DuckCatalog>();

		if (catalog.GetIsEncrypted()) {
			return FlushEncrypted();
		}

		auto data = memory_stream.GetData();
		auto size = memory_stream.GetPosition();
		// compute the checksum over the entry
		auto checksum = Checksum(data, size);
		// write the checksum and the length of the entry
		stream->Write<uint64_t>(size);
		stream->Write<uint64_t>(checksum);
		// write data to the underlying stream
		stream->WriteData(memory_stream.GetData(), memory_stream.GetPosition());
		// rewind the buffer
		memory_stream.Rewind();
	}

	void FlushEncrypted() {
		auto &catalog = wal.GetDatabase().GetCatalog().Cast<DuckCatalog>();
		auto encryption_key_id = catalog.GetEncryptionKeyId();

		auto data = memory_stream.GetData();
		auto size = memory_stream.GetPosition();

		// compute the checksum over the entry
		auto checksum = Checksum(data, size);

		auto &db = wal.GetDatabase();
		auto &keys = EncryptionKeyManager::Get(db.GetDatabase());
		auto metadata = make_uniq<EncryptionStateMetadata>(db.GetStorageManager().GetCipher(),
		                                                   MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH,
		                                                   EncryptionTypes::EncryptionVersion::V0_1);
		auto encryption_state =
		    db.GetDatabase().GetEncryptionUtil(db.IsReadOnly())->CreateEncryptionState(std::move(metadata));

		// temp buffer
		const idx_t ciphertext_size = size + sizeof(uint64_t);
		std::unique_ptr<uint8_t[]> temp_buf(new uint8_t[ciphertext_size]);

		EncryptionNonce nonce(db.GetStorageManager().GetCipher(), db.GetStorageManager().GetEncryptionVersion());
		EncryptionTag tag;

		// generate nonce
		encryption_state->GenerateRandomData(nonce.data(), nonce.size());

		stream->Write<uint64_t>(size);
		stream->WriteData(nonce.data(), nonce.size());

		//! store the checksum in the temp buffer
		memcpy(temp_buf.get(), &checksum, sizeof(checksum));
		//! checksum + entry in the temp buf
		memcpy(temp_buf.get() + sizeof(checksum), memory_stream.GetData(), memory_stream.GetPosition());

		//! encrypt the temp buf
		encryption_state->InitializeEncryption(nonce, keys.GetKey(encryption_key_id));
		encryption_state->Process(temp_buf.get(), ciphertext_size, temp_buf.get(), ciphertext_size);

		//! calculate the tag (for GCM)
		encryption_state->Finalize(temp_buf.get(), ciphertext_size, tag.data(), tag.size());

		// write data to the underlying stream
		stream->WriteData(temp_buf.get(), ciphertext_size);

		// Write the tag to the stream
		if (encryption_state->GetCipher() == EncryptionTypes::CipherType::GCM) {
			D_ASSERT(!tag.IsAllZeros());
			stream->WriteData(tag.data(), tag.size());
		}

		// rewind the buffer
		memory_stream.Rewind();
	}

	WriteAheadLog &GetWAL() {
		return wal;
	}

private:
	WriteAheadLog &wal;
	optional_ptr<WriteStream> stream;
	MemoryStream memory_stream;
};

class WriteAheadLogSerializer {
public:
	WriteAheadLogSerializer(WriteAheadLog &wal, WALType wal_type)
	    : checksum_writer(wal), serializer(checksum_writer, SerializationOptions(wal.GetDatabase())) {
		if (!wal.Initialized()) {
			wal.Initialize();
		}
		// Write a header, if none has been written yet.
		wal.WriteHeader();
		serializer.Begin();
		serializer.WriteProperty(100, "wal_type", wal_type);
	}

	void End() {
		serializer.End();
		checksum_writer.Flush();
		checksum_writer.GetWAL().IncrementWALEntriesCount();
	}

	template <class T>
	void WriteProperty(const field_id_t field_id, const char *tag, const T &value) {
		serializer.WriteProperty(field_id, tag, value);
	}

	//! Serialize a generated WAL entry struct (its fields follow the WALType marker written in the constructor)
	template <class T>
	void WriteEntry(const T &entry) {
		entry.Serialize(serializer);
	}

	template <class FUNC>
	void WriteList(const field_id_t field_id, const char *tag, idx_t count, FUNC func) {
		serializer.WriteList(field_id, tag, count, func);
	}

private:
	ChecksumWriter checksum_writer;
	SerializationOptions options;
	BinarySerializer serializer;
};

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteHeader() {
	D_ASSERT(writer);
	if (writer->GetFileSize() > 0) {
		// Already written - no need to write a header.
		return;
	}

	// Write the header containing
	// - the version marker,
	// - the header_id of the matching database file, and
	// - the checkpoint iteration of the matching database file.
	// Note that we explicitly do not checksum the header, as it contains the version entry.

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::WAL_VERSION);

	auto &database = GetDatabase();
	auto &catalog = database.GetCatalog().Cast<DuckCatalog>();
	auto encryption_version_number =
	    catalog.GetIsEncrypted() ? idx_t(WAL_ENCRYPTED_VERSION_NUMBER) : idx_t(WAL_VERSION_NUMBER);
	serializer.WriteProperty(101, "version", encryption_version_number);

	auto &single_file_block_manager = database.GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
	auto file_version_number = single_file_block_manager.GetVersionNumber();
	// double check
	if (StorageManager::TargetAtLeastVersion(StorageVersion::V1_3_0, file_version_number)) {
		auto db_identifier = single_file_block_manager.GetDBIdentifier();
		serializer.WriteList(102, "db_identifier", MainHeader::DB_IDENTIFIER_LEN,
		                     [&](Serializer::List &list, idx_t i) { list.WriteElement(db_identifier[i]); });
		idx_t current_checkpoint_iteration;
		if (checkpoint_iteration.IsValid()) {
			current_checkpoint_iteration = checkpoint_iteration.GetIndex();
		} else {
			current_checkpoint_iteration = single_file_block_manager.GetCheckpointIteration();
		}
		serializer.WriteProperty(103, "checkpoint_iteration", current_checkpoint_iteration);
	}

	serializer.End();
}

void WriteAheadLog::WriteCheckpoint(MetaBlockPointer meta_block) {
	WriteAheadLogSerializer serializer(*this, WALType::CHECKPOINT);
	serializer.WriteEntry(WALCheckpoint {meta_block});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(const TableCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TABLE);
	serializer.WriteEntry(WALCreateTable {entry.GetInfo()});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(const TableCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TABLE);
	serializer.WriteEntry(WALDropTable {entry.schema.name, entry.name});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(const SchemaCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_SCHEMA);
	// serialize the schema as a QualifiedName: parent schemas form the path, the schema name is the name. For storage
	// versions older than v2.0.0 (which only support top-level schemas) the legacy "schema" name field is written.
	vector<Identifier> parent_schemas;
	auto parent = entry.GetParentSchema();
	while (parent) {
		parent_schemas.push_back(parent->name);
		parent = parent->GetParentSchema();
	}
	std::reverse(parent_schemas.begin(), parent_schemas.end());
	serializer.WriteEntry(WALCreateSchema {entry.name, QualifiedName(std::move(parent_schemas), entry.name)});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(const SequenceCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_SEQUENCE);
	serializer.WriteEntry(WALCreateSequence {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropSequence(const SequenceCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_SEQUENCE);
	serializer.WriteEntry(WALDropSequence {entry.schema.name, entry.name});
	serializer.End();
}

void WriteAheadLog::WriteSequenceValue(SequenceValue val) {
	auto &sequence = *val.entry;
	WriteAheadLogSerializer serializer(*this, WALType::SEQUENCE_VALUE);
	// last_value (id 105) is only serialized from storage version v2.0.0 onwards, and is omitted when unset
	serializer.WriteEntry(WALSequenceValue {sequence.schema.name, sequence.name, val.usage_count, val.counter,
	                                        val.entry->GetData().last_value});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// MACROS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateMacro(const ScalarMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_MACRO);
	serializer.WriteEntry(WALCreateMacro {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropMacro(const ScalarMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_MACRO);
	serializer.WriteEntry(WALDropMacro {entry.schema.name, entry.name});
	serializer.End();
}

void WriteAheadLog::WriteCreateTableMacro(const TableMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TABLE_MACRO);
	serializer.WriteEntry(WALCreateTableMacro {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropTableMacro(const TableMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TABLE_MACRO);
	serializer.WriteEntry(WALDropTableMacro {entry.schema.name, entry.name});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//

void SerializeIndex(AttachedDatabase &db, WriteAheadLogSerializer &serializer, TableIndexList &list,
                    const Identifier &name) {
	case_insensitive_map_t<Value> options;
	auto storage_version = db.GetStorageManager().GetStorageVersion();
	// Before: serialization version 3
	auto v1_0_0_storage = StorageManager::IsPriorToVersion(StorageVersion::V1_2_0, storage_version);
	if (!v1_0_0_storage) {
		options["v1_0_0_storage"] = v1_0_0_storage;
	}

	for (auto &index : list.Indexes()) {
		if (name == index.GetIndexName()) {
			// We never write an unbound index to the WAL.
			D_ASSERT(index.IsBound());
			const auto &info = index.Cast<BoundIndex>().SerializeToWAL(options);
			serializer.WriteProperty(102, "index_storage_info", info);
			serializer.WriteList(103, "index_storage", info.buffers.size(), [&](Serializer::List &list, idx_t i) {
				auto &buffers = info.buffers[i];
				for (auto buffer : buffers) {
					list.WriteElement(buffer.buffer_ptr, buffer.allocation_size);
				}
			});
			break;
		}
	}
}

void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_INDEX);
	serializer.WriteProperty(101, "index_catalog_entry", &entry);

	// Serialize the index data to the persistent storage and write the metadata.
	auto &index_entry = entry.Cast<DuckIndexEntry>();
	auto &list = index_entry.GetDataTableInfo().GetIndexes();
	auto &database = GetDatabase();
	SerializeIndex(database, serializer, list, index_entry.name);
	serializer.End();
}

void WriteAheadLog::WriteDropIndex(const IndexCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_INDEX);
	serializer.WriteEntry(WALDropIndex {entry.schema.name, entry.name});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(const TypeCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TYPE);
	serializer.WriteEntry(WALCreateType {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropType(const TypeCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TYPE);
	serializer.WriteEntry(WALDropType {entry.schema.name, entry.name});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// TRIGGERS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTrigger(const TriggerCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TRIGGER);
	serializer.WriteEntry(WALCreateTrigger {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropTrigger(const TriggerCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TRIGGER);
	serializer.WriteEntry(WALDropTrigger {entry.schema.name, entry.name, entry.base_table->Table()});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(const ViewCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_VIEW);
	serializer.WriteEntry(WALCreateView {entry.GetInfo()});
	serializer.End();
}

void WriteAheadLog::WriteDropView(const ViewCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_VIEW);
	serializer.WriteEntry(WALDropView {entry.schema.name, entry.name});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(const SchemaCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_SCHEMA);
	// serialize the schema as a QualifiedName: parent schemas form the path, the schema name is the name. For storage
	// versions older than v2.0.0 (which only support top-level schemas) the legacy "schema" name field is written.
	vector<Identifier> parent_schemas;
	auto parent = entry.GetParentSchema();
	while (parent) {
		parent_schemas.push_back(parent->name);
		parent = parent->GetParentSchema();
	}
	std::reverse(parent_schemas.begin(), parent_schemas.end());
	serializer.WriteEntry(WALDropSchema {entry.name, QualifiedName(std::move(parent_schemas), entry.name)});
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(const Identifier &schema, const Identifier &table) {
	WriteAheadLogSerializer serializer(*this, WALType::USE_TABLE);
	serializer.WriteEntry(WALUseTable {schema, table});
	serializer.End();
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	chunk.Verify(GetDatabase().GetDatabase());

	WriteAheadLogSerializer serializer(*this, WALType::INSERT_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteRowGroupData(const PersistentCollectionData &data) {
	D_ASSERT(!data.row_group_data.empty());

	WriteAheadLogSerializer serializer(*this, WALType::ROW_GROUP_DATA);
	serializer.WriteProperty(101, "row_group_data", data);
	serializer.End();

	// mark written blocks as checkpointed
	auto &block_manager = GetDatabase().GetStorageManager().GetBlockManager();
	for (auto &block_id : data.GetBlockIds()) {
		block_manager.MarkBlockAsCheckpointed(block_id);
	}
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify(GetDatabase().GetDatabase());

	WriteAheadLogSerializer serializer(*this, WALType::DELETE_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify(GetDatabase().GetDatabase());

	WriteAheadLogSerializer serializer(*this, WALType::UPDATE_TUPLE);
	serializer.WriteProperty(101, "column_indexes", column_indexes);
	serializer.WriteProperty(102, "chunk", chunk);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Write ALTER Statement
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteAlter(CatalogEntry &entry, const AlterInfo &info) {
	WriteAheadLogSerializer serializer(*this, WALType::ALTER_INFO);
	serializer.WriteProperty(101, "info", &info);

	if (!info.IsAddPrimaryKey()) {
		return serializer.End();
	}

	auto &table_info = info.Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	auto &unique = constraint_info.constraint->Cast<UniqueConstraint>();

	auto &table_entry = entry.Cast<DuckTableEntry>();
	auto &parent = table_entry.Parent().Cast<DuckTableEntry>();
	auto &parent_info = parent.GetStorage().GetDataTableInfo();
	auto &list = parent_info->GetIndexes();

	auto name = unique.GetName(parent.name);
	auto &database = GetDatabase();
	SerializeIndex(database, serializer, list, name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	if (!writer) {
		return;
	}

	// write an empty entry
	WriteAheadLogSerializer serializer(*this, WALType::WAL_FLUSH);
	serializer.End();

	// flushes all changes made to the WAL to disk
	writer->Sync();
	storage_manager.SetWALSize(writer->GetFileSize());
}

idx_t WriteAheadLog::FlushAppendNoSync() {
	if (!writer) {
		return 0;
	}

	// write an empty entry, marking the end of this commit in the WAL byte stream
	WriteAheadLogSerializer serializer(*this, WALType::WAL_FLUSH);
	serializer.End();

	// push the buffered bytes into the OS page cache, but do NOT fsync yet -- the grouped fsync in GroupSync makes
	// them durable. Appends are serialized under the WAL lock, so the returned offset covers exactly this commit's
	// entries (and every earlier commit's), and the publish below is monotonic with a plain release store.
	writer->Flush();
	auto offset = writer->GetFileSize();
	storage_manager.SetWALSize(offset);
	flushed_offset.store(offset, std::memory_order_release);
	return offset;
}

void WriteAheadLog::BumpSyncEpochNotify() {
	{
		// the epoch advances under the same mutex that guards the waiters' predicate check, so a waiter either sees
		// the new epoch and does not park, or is parked and receives the notify; notifying after the unlock keeps
		// woken waiters from immediately colliding with a held mutex
		std::lock_guard<std::mutex> guard(sync_wait_mutex);
		sync_epoch.fetch_add(1, std::memory_order_acq_rel);
	}
	sync_wait_cv.notify_all();
}

void WriteAheadLog::WaitSyncEpochChange(uint64_t current) {
	std::unique_lock<std::mutex> guard(sync_wait_mutex);
	sync_wait_cv.wait(guard, [&]() { return sync_epoch.load(std::memory_order_acquire) != current; });
}

void WriteAheadLog::GroupSync(idx_t my_offset) {
	if (!writer || my_offset == 0) {
		return;
	}
	for (;;) {
		// the epoch must be read BEFORE the durable/failed checks: a bump between those checks and the park then
		// makes the park return immediately instead of missing the update
		uint64_t epoch = sync_epoch.load(std::memory_order_acquire);
		if (durable_offset.load(std::memory_order_acquire) >= my_offset) {
			return;
		}
		if (sync_failed.load(std::memory_order_acquire)) {
			throw FatalException("Failed to sync WAL during commit: an earlier WAL sync failed, durability can no "
			                     "longer be guaranteed");
		}
		// A committer fsyncs itself -- overlapping with any fsyncs already in flight, up to the file system's declared
		// sync parallelism -- unless an in-flight fsync's target already covers its bytes, in which case
		// it parks until durability advances. Overlapping fsyncs pipeline the device/network round trips (a commit
		// arriving mid-fsync does not wait out someone else's round trip); committers beyond the cap park, and the
		// next fsync's late-snapped target covers them (group commit, no delay).
		idx_t lanes = active_syncs.load(std::memory_order_acquire);
		if (lanes > 0 && syncing_target.load(std::memory_order_acquire) >= my_offset) {
			// if the covering fsync completes between the two loads, its epoch bump makes this park return immediately
			WaitSyncEpochChange(epoch);
			continue;
		}
		if (lanes >= sync_lane_cap || !active_syncs.compare_exchange_strong(lanes, lanes + 1, std::memory_order_acq_rel,
		                                                                    std::memory_order_acquire)) {
			WaitSyncEpochChange(epoch);
			continue;
		}
		// cover every byte flushed (under the WAL lock) up to now -- includes our own bytes
		uint64_t gen = truncate_gen.load(std::memory_order_acquire);
		idx_t target = flushed_offset.load(std::memory_order_acquire);
		idx_t prev_syncing = syncing_target.load(std::memory_order_relaxed);
		while (prev_syncing < target &&
		       !syncing_target.compare_exchange_weak(prev_syncing, target, std::memory_order_release,
		                                             std::memory_order_relaxed)) {
		}
		try {
			writer->handle->Sync();
		} catch (const std::exception &ex) {
			// A failed fsync is unrecoverable: the OS may have dropped the failed dirty pages as clean, so a
			// retried fsync could falsely report success for bytes that never reached disk. Poison the WAL so
			// every pending and future committer fails, and escalate to a fatal error so the database is
			// invalidated and recovers from the durable WAL prefix on reopen.
			sync_failed.store(true, std::memory_order_release);
			active_syncs.fetch_sub(1, std::memory_order_acq_rel);
			BumpSyncEpochNotify();
			ErrorData error(ex);
			throw FatalException("Failed to sync WAL during commit. Cannot continue operation.\nError: %s",
			                     error.Message());
		}
		// raise durable_offset to (at least) this fsync's target; concurrent fsyncs may race, take the max.
		// If a truncate intervened, the snapshotted target may exceed the truncation point -- discard the raise
		// and loop; our own marker (if still valid) is covered by a fresh fsync with a fresh target.
		if (truncate_gen.load(std::memory_order_acquire) == gen) {
			idx_t durable = durable_offset.load(std::memory_order_relaxed);
			while (durable < target && !durable_offset.compare_exchange_weak(durable, target, std::memory_order_release,
			                                                                 std::memory_order_relaxed)) {
			}
		}
		active_syncs.fetch_sub(1, std::memory_order_acq_rel);
		BumpSyncEpochNotify();
	}
}

void WriteAheadLog::IncrementWALEntriesCount() {
	storage_manager.IncrementWALEntriesCount();
}

} // namespace duckdb
