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
#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/wal_entry.hpp"

#include <chrono>
#include <thread>

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
		auto buffer_size = DBConfig::GetConfig(GetDatabase().GetDatabase()).options.wal_buffer_size;
		writer =
		    make_uniq<BufferedFileWriter>(FileSystem::Get(GetDatabase()), wal_path,
		                                  FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                      FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS,
		                                  QueryContext(), buffer_size);
		if (init_state == WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE) {
			writer->Truncate(storage_manager.GetWALSize());
		} else {
			storage_manager.SetWALSize(writer->GetFileSize());
		}
		init_state = WALInitState::INITIALIZED;
	}
	return *writer;
}

idx_t WriteAheadLog::GetTotalWritten() const {
	if (!Initialized()) {
		return 0;
	}
	// serialize against the group commit sync leader's buffer push (writer->Flush() under flush_lock), which mutates
	// the same offset/total_written fields this read sums
	lock_guard<mutex> flush_guard(flush_lock);
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
	idx_t file_size;
	{
		// serialize the truncate (which removes reverted/uncommitted WAL entries) against the group commit sync
		// leader's push-to-OS, which runs without the storage manager WAL lock (see LockFlush())
		auto flush_guard = LockFlush();
		writer->Truncate(size);
		file_size = writer->GetFileSize();
	}
	storage_manager.SetWALSize(file_size);
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

		// serialize this entry's writes into the WAL buffer against the group commit sync leader's push-to-OS
		// (which takes the same flush lock) - see WriteAheadLog::LockFlush()
		auto flush_guard = wal.LockFlush();
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

		// serialize this entry's writes into the WAL buffer against the sync leader's push-to-OS (see LockFlush())
		auto flush_guard = wal.LockFlush();
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
	auto flush_guard = LockFlush();
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
	auto target_offset = WriteFlushMarker();
	// the caller holds the WAL lock, so no concurrent appends can arrive - no point waiting for a batch
	SyncUpTo(target_offset, false);
}

idx_t WriteAheadLog::WriteFlushMarker(bool requires_block_sync, bool push_to_os) {
	if (!writer) {
		return 0;
	}

	// write an empty entry (appended to the in-memory WAL buffer under flush_lock, via ChecksumWriter::Flush)
	WriteAheadLogSerializer serializer(*this, WALType::WAL_FLUSH);
	serializer.End();

	idx_t target_offset;
	idx_t file_size;
	{
		// Under flush_lock: (optionally) push the buffered data to the OS now, and snapshot + advance written_offset.
		// Holding flush_lock keeps the push and the written_offset update atomic with respect to the sync leader
		// (which also pushes the buffer + reads written_offset under flush_lock). In the deferred path
		// (push_to_os=false) the bytes stay buffered until the sync leader flushes them.
		auto flush_guard = LockFlush();
		if (push_to_os) {
			// synchronous path: push all buffered data to the operating system now (the fsync happens in SyncUpTo)
			writer->Flush();
		}
		// note: GetTotalWritten() is the logical byte count (buffered + flushed), the same whether or not we pushed
		target_offset = writer->GetTotalWritten();
		file_size = writer->GetFileSize();
		if (requires_block_sync) {
			// record that this marker references optimistically written row group data
			// this must happen BEFORE written_offset advances: any sync leader whose fsync covers this marker
			// must observe the block sync requirement (see SyncUpTo)
			block_sync_pending_offset = MaxValue<idx_t>(block_sync_pending_offset.load(), target_offset);
		}
		written_offset = target_offset;
	}
	storage_manager.SetWALSize(file_size);
	return target_offset;
}

void WriteAheadLog::SyncUpTo(idx_t target_offset, bool wait_for_batch, bool leader_pushes_batch) {
	std::unique_lock<mutex> lock(sync_mutex);
	while (synced_offset < target_offset) {
		if (sync_failed) {
			// A previous batched fsync failed. The OS may have dropped the failed dirty pages (marking them clean)
			// without them reaching disk, so a retried fsync could falsely report success for flush markers that
			// were never persisted. Those markers cannot be truncated anymore - fail every waiter instead of
			// acknowledging durability that cannot be guaranteed. (Targets covered by an earlier successful fsync
			// do not reach this point: synced_offset already covers them.)
			throw FatalException(
			    "Failed to sync WAL during commit: an earlier WAL sync failed, durability can no longer be "
			    "guaranteed");
		}
		if (sync_in_progress) {
			// another thread is currently performing an fsync - wait for it to finish
			// its fsync might not cover our target offset (it could have started before our data was written),
			// in which case we loop around and either become the next leader or wait again
			sync_waiters++;
			sync_cv.wait(lock);
			sync_waiters--;
			continue;
		}
		// no fsync in progress - this thread becomes the sync leader
		// the fsync covers everything pushed to the operating system up to this point, including the flush
		// markers of other concurrently committing transactions - they share this single fsync (group commit)
		sync_in_progress = true;
		bool do_batch_wait = wait_for_batch && batch_commits;
		lock.unlock();
		auto sync_target = written_offset.load();
		if (do_batch_wait) {
			// Other transactions were committing concurrently during the previous fsync.
			// Before fsync-ing, give concurrently committing transactions a window to finish appending their
			// flush markers, so that this fsync covers them as well and they do not have to wait for the next
			// one (micro-batching, similar in spirit to PostgreSQL's commit_delay).
			// The maximum window is group_commit_delay microseconds; with -1 (automatic, the
			// default) it is scaled to a fraction of the observed fsync duration: a wait that is small relative
			// to the fsync adds little commit latency, while letting (at best) an entire round of concurrent
			// committers share this fsync. The wait stops early as soon as no new appends arrive.
			auto delay_micros = Settings::Get<GroupCommitDelaySetting>(GetDatabase().GetDatabase());
			if (delay_micros < 0) {
				delay_micros = static_cast<int64_t>(sync_duration_micros.load() / 4);
			}
			if (delay_micros > 0) {
				auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(delay_micros);
				// note that the effective poll granularity is limited by the operating system timer resolution
				auto poll_interval = std::chrono::microseconds(MaxValue<int64_t>(delay_micros / 10, 20));
				while (true) {
					std::this_thread::sleep_for(poll_interval);
					auto new_target = written_offset.load();
					if (new_target == sync_target) {
						// no new appends - stop waiting
						break;
					}
					sync_target = new_target;
					if (std::chrono::steady_clock::now() >= deadline) {
						break;
					}
				}
			}
		}
		if (leader_pushes_batch && writer) {
			// Deferred group commit: committers only appended their flush markers to the in-memory WAL buffer
			// (WriteFlushMarker with push_to_os=false). The sync leader now pushes the whole accumulated batch to
			// the operating system in a single write(), so the per-write round-trip is shared across all batched
			// commits instead of paid per commit. The push runs under the WAL flush_lock - NOT the storage manager WAL
			// lock - so it cannot deadlock against a checkpoint / catalog commit / synchronous commit that holds the
			// WAL lock and waits. Pushing the whole buffer is safe: deferred commits write their WAL entries only after
			// ValidateCommitConflicts (so they cannot be reverted/truncated), and any not-yet-marked trailing bytes
			// are ignored on crash recovery (replay stops at the last flush marker). sync_target stays at the last
			// committed marker, so durability is only ever promised up to a committed point.
			lock_guard<mutex> flush_guard(flush_lock);
			writer->Flush();
			sync_target = written_offset.load();
		}
		ErrorData error;
		try {
			if (block_sync_pending_offset.load() > block_synced_offset.load()) {
				// one or more flush markers covered by this fsync reference optimistically written row group
				// data: the referenced blocks must be durable in the database file BEFORE the WAL fsync makes
				// those markers durable. Perform one (batched) database file sync covering all of them.
				// A marker above sync_target conservatively triggers another sync in a later round.
				storage_manager.GetBlockManager().FileSync();
				block_synced_offset = MaxValue<idx_t>(block_synced_offset.load(), sync_target);
			}
			auto sync_start = std::chrono::steady_clock::now();
			writer->SyncData();
			// update the moving average of the fsync duration (drives the automatic micro-batching window)
			auto sync_micros = static_cast<idx_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - sync_start)
			        .count());
			auto previous = sync_duration_micros.load();
			sync_duration_micros = previous == 0 ? sync_micros : (3 * previous + sync_micros) / 4;
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}
		// detect concurrent commit activity: did other transactions append while we were syncing?
		auto post_sync_written = written_offset.load();
		lock.lock();
		sync_in_progress = false;
		// adaptive micro-batching: only wait for a batch to form when there is concurrent commit activity,
		// i.e. other transactions are waiting on this fsync or appended to the WAL while we were syncing
		batch_commits = sync_waiters > 0 || post_sync_written > sync_target;
		if (!error.HasError()) {
			synced_offset = MaxValue(synced_offset, sync_target);
		} else if (leader_pushes_batch) {
			// deferred (group) commit: the failed fsync covered other commits' non-truncatable flush markers -
			// poison the sync state so no waiter acknowledges durability based on a retried fsync (see above).
			// The exclusive path is not poisoned: a failed synchronous commit truncates its own bytes on revert,
			// so later commits fsync fresh pages and can genuinely succeed.
			sync_failed = true;
		}
		sync_cv.notify_all();
		if (error.HasError()) {
			error.Throw();
		}
	}
}

void WriteAheadLog::IncrementWALEntriesCount() {
	storage_manager.IncrementWALEntriesCount();
}

} // namespace duckdb
