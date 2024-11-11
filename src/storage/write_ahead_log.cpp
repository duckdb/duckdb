#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

constexpr uint64_t WAL_VERSION_NUMBER = 2;

WriteAheadLog::WriteAheadLog(AttachedDatabase &database, const string &wal_path, idx_t wal_size,
                             WALInitState init_state)
    : database(database), wal_path(wal_path), wal_size(wal_size), init_state(init_state) {
}

WriteAheadLog::~WriteAheadLog() {
}

BufferedFileWriter &WriteAheadLog::Initialize() {
	if (Initialized()) {
		return *writer;
	}
	lock_guard<mutex> lock(wal_lock);
	if (!writer) {
		writer = make_uniq<BufferedFileWriter>(FileSystem::Get(database), wal_path,
		                                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                           FileFlags::FILE_FLAGS_APPEND);
		if (init_state == WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE) {
			writer->Truncate(wal_size);
		}
		wal_size = writer->GetFileSize();
		init_state = WALInitState::INITIALIZED;
	}
	return *writer;
}

//! Gets the total bytes written to the WAL since startup
idx_t WriteAheadLog::GetWALSize() const {
	D_ASSERT(init_state != WALInitState::NO_WAL || wal_size == 0);
	return wal_size;
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
		wal_size = size;
		return;
	}
	writer->Truncate(size);
	wal_size = writer->GetFileSize();
}

bool WriteAheadLog::Initialized() const {
	return init_state == WALInitState::INITIALIZED;
}

void WriteAheadLog::Delete() {
	if (init_state == WALInitState::NO_WAL) {
		// no WAL to delete
		return;
	}
	writer.reset();
	auto &fs = FileSystem::Get(database);
	fs.RemoveFile(wal_path);
	init_state = WALInitState::NO_WAL;
	wal_size = 0;
}

//===--------------------------------------------------------------------===//
// Serializer
//===--------------------------------------------------------------------===//
class ChecksumWriter : public WriteStream {
public:
	explicit ChecksumWriter(WriteAheadLog &wal) : wal(wal) {
	}

	void WriteData(const_data_ptr_t buffer, idx_t write_size) override {
		// buffer data into the memory stream
		memory_stream.WriteData(buffer, write_size);
	}

	void Flush() {
		if (!stream) {
			stream = wal.Initialize();
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

private:
	WriteAheadLog &wal;
	optional_ptr<WriteStream> stream;
	MemoryStream memory_stream;
};

class WriteAheadLogSerializer {
public:
	WriteAheadLogSerializer(WriteAheadLog &wal, WALType wal_type) : checksum_writer(wal), serializer(checksum_writer) {
		if (!wal.Initialized()) {
			wal.Initialize();
		}
		// write a version marker if none has been written yet
		wal.WriteVersion();
		serializer.Begin();
		serializer.WriteProperty(100, "wal_type", wal_type);
	}

	void End() {
		serializer.End();
		checksum_writer.Flush();
	}

	template <class T>
	void WriteProperty(const field_id_t field_id, const char *tag, const T &value) {
		serializer.WriteProperty(field_id, tag, value);
	}

	template <class FUNC>
	void WriteList(const field_id_t field_id, const char *tag, idx_t count, FUNC func) {
		serializer.WriteList(field_id, tag, count, func);
	}

private:
	ChecksumWriter checksum_writer;
	BinarySerializer serializer;
};

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteVersion() {
	D_ASSERT(writer);
	if (writer->GetFileSize() > 0) {
		// already written - no need to write a version marker
		return;
	}
	// write the version marker
	// note that we explicitly do not checksum the version entry
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::WAL_VERSION);
	serializer.WriteProperty(101, "version", idx_t(WAL_VERSION_NUMBER));
	serializer.End();
}

void WriteAheadLog::WriteCheckpoint(MetaBlockPointer meta_block) {
	WriteAheadLogSerializer serializer(*this, WALType::CHECKPOINT);
	serializer.WriteProperty(101, "meta_block", meta_block);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(const TableCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TABLE);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(const TableCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TABLE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(const SchemaCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(const SequenceCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_SEQUENCE);
	serializer.WriteProperty(101, "sequence", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropSequence(const SequenceCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_SEQUENCE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteSequenceValue(SequenceValue val) {
	auto &sequence = *val.entry;
	WriteAheadLogSerializer serializer(*this, WALType::SEQUENCE_VALUE);
	serializer.WriteProperty(101, "schema", sequence.schema.name);
	serializer.WriteProperty(102, "name", sequence.name);
	serializer.WriteProperty(103, "usage_count", val.usage_count);
	serializer.WriteProperty(104, "counter", val.counter);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// MACROS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateMacro(const ScalarMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_MACRO);
	serializer.WriteProperty(101, "macro", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropMacro(const ScalarMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteCreateTableMacro(const TableMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TABLE_MACRO);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropTableMacro(const TableMacroCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TABLE_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//

void SerializeIndex(AttachedDatabase &db, WriteAheadLogSerializer &serializer, TableIndexList &list,
                    const string &name) {
	const auto &db_options = db.GetDatabase().config.options;
	auto v1_0_0_storage = db_options.serialization_compatibility.serialization_version < 3;
	case_insensitive_map_t<Value> options;
	if (!v1_0_0_storage) {
		options.emplace("v1_0_0_storage", v1_0_0_storage);
	}

	list.Scan([&](Index &index) {
		if (name == index.GetIndexName()) {
			// We never write an unbound index to the WAL.
			D_ASSERT(index.IsBound());

			const auto &info = index.Cast<BoundIndex>().GetStorageInfo(options, true);
			serializer.WriteProperty(102, "index_storage_info", info);
			serializer.WriteList(103, "index_storage", info.buffers.size(), [&](Serializer::List &list, idx_t i) {
				auto &buffers = info.buffers[i];
				for (auto buffer : buffers) {
					list.WriteElement(buffer.buffer_ptr, buffer.allocation_size);
				}
			});
			return true;
		}
		return false;
	});
}

void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_INDEX);
	serializer.WriteProperty(101, "index_catalog_entry", &entry);

	// Serialize the index data to the persistent storage and write the metadata.
	auto &index_entry = entry.Cast<DuckIndexEntry>();
	auto &list = index_entry.GetDataTableInfo().GetIndexes();
	SerializeIndex(database, serializer, list, index_entry.name);
	serializer.End();
}

void WriteAheadLog::WriteDropIndex(const IndexCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_INDEX);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(const TypeCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_TYPE);
	serializer.WriteProperty(101, "type", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropType(const TypeCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_TYPE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(const ViewCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::CREATE_VIEW);
	serializer.WriteProperty(101, "view", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropView(const ViewCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_VIEW);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(const SchemaCatalogEntry &entry) {
	WriteAheadLogSerializer serializer(*this, WALType::DROP_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(const string &schema, const string &table) {
	WriteAheadLogSerializer serializer(*this, WALType::USE_TABLE);
	serializer.WriteProperty(101, "schema", schema);
	serializer.WriteProperty(102, "table", table);
	serializer.End();
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	chunk.Verify();

	WriteAheadLogSerializer serializer(*this, WALType::INSERT_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteRowGroupData(const PersistentCollectionData &data) {
	D_ASSERT(!data.row_group_data.empty());

	WriteAheadLogSerializer serializer(*this, WALType::ROW_GROUP_DATA);
	serializer.WriteProperty(101, "row_group_data", data);
	serializer.End();
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify();

	WriteAheadLogSerializer serializer(*this, WALType::DELETE_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify();

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
	wal_size = writer->GetFileSize();
}

} // namespace duckdb
