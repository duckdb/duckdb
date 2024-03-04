#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

const uint64_t WAL_VERSION_NUMBER = 2;

WriteAheadLog::WriteAheadLog(AttachedDatabase &database, const string &path) : skip_writing(false), database(database) {
	wal_path = path;
	writer = make_uniq<BufferedFileWriter>(FileSystem::Get(database), path.c_str(),
	                                       NumericCast<uint8_t>(FileFlags::FILE_FLAGS_WRITE |
	                                                            FileFlags::FILE_FLAGS_FILE_CREATE |
	                                                            FileFlags::FILE_FLAGS_APPEND));
}

WriteAheadLog::~WriteAheadLog() {
}

int64_t WriteAheadLog::GetWALSize() {
	D_ASSERT(writer);
	return writer->GetFileSize();
}

idx_t WriteAheadLog::GetTotalWritten() {
	D_ASSERT(writer);
	return writer->GetTotalWritten();
}

void WriteAheadLog::Truncate(int64_t size) {
	writer->Truncate(size);
}

void WriteAheadLog::Delete() {
	if (!writer) {
		return;
	}
	writer.reset();

	auto &fs = FileSystem::Get(database);
	fs.RemoveFile(wal_path);
}

//===--------------------------------------------------------------------===//
// Serializer
//===--------------------------------------------------------------------===//
class ChecksumWriter : public WriteStream {
public:
	explicit ChecksumWriter(WriteAheadLog &wal) : wal(wal), stream(wal.GetWriter()) {
	}

	void WriteData(const_data_ptr_t buffer, idx_t write_size) override {
		if (wal.skip_writing) {
			return;
		}
		// buffer data into the memory stream
		memory_stream.WriteData(buffer, write_size);
	}

	void Flush() {
		if (wal.skip_writing) {
			return;
		}
		auto data = memory_stream.GetData();
		auto size = memory_stream.GetPosition();
		// compute the checksum over the entry
		auto checksum = Checksum(data, size);
		// write the checksum and the length of the entry
		stream.Write<uint64_t>(size);
		stream.Write<uint64_t>(checksum);
		// write data to the underlying stream
		stream.WriteData(memory_stream.GetData(), memory_stream.GetPosition());
		// rewind the buffer
		memory_stream.Rewind();
	}

private:
	WriteAheadLog &wal;
	WriteStream &stream;
	MemoryStream memory_stream;
};

class WriteAheadLogSerializer {
public:
	WriteAheadLogSerializer(WriteAheadLog &wal, WALType wal_type)
	    : wal(wal), checksum_writer(wal), serializer(checksum_writer) {
		if (wal.skip_writing) {
			return;
		}
		// write a version marker if none has been written yet
		wal.WriteVersion();
		serializer.Begin();
		serializer.WriteProperty(100, "wal_type", wal_type);
	}

	void End() {
		if (wal.skip_writing) {
			return;
		}
		serializer.End();
		checksum_writer.Flush();
	}

	template <class T>
	void WriteProperty(const field_id_t field_id, const char *tag, const T &value) {
		if (wal.skip_writing) {
			return;
		}
		serializer.WriteProperty(field_id, tag, value);
	}

	template <class FUNC>
	void WriteList(const field_id_t field_id, const char *tag, idx_t count, FUNC func) {
		if (wal.skip_writing) {
			return;
		}
		serializer.WriteList(field_id, tag, count, func);
	}

private:
	WriteAheadLog &wal;
	ChecksumWriter checksum_writer;
	BinarySerializer serializer;
};

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteVersion() {
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

void WriteAheadLog::WriteSequenceValue(const SequenceCatalogEntry &entry, SequenceValue val) {
	WriteAheadLogSerializer serializer(*this, WALType::SEQUENCE_VALUE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
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

void SerializeIndexToWAL(WriteAheadLogSerializer &serializer, const unique_ptr<Index> &index) {

	auto index_storage_info = index->GetStorageInfo(true);
	serializer.WriteProperty(102, "index_storage_info", index_storage_info);

	serializer.WriteList(103, "index_storage", index_storage_info.buffers.size(), [&](Serializer::List &list, idx_t i) {
		auto &buffers = index_storage_info.buffers[i];
		for (auto buffer : buffers) {
			list.WriteElement(buffer.buffer_ptr, buffer.allocation_size);
		}
	});
}

void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}

	WriteAheadLogSerializer serializer(*this, WALType::CREATE_INDEX);
	serializer.WriteProperty(101, "index_catalog_entry", &entry);

	// now serialize the index data to the persistent storage and write the index metadata
	auto &duck_index_entry = entry.Cast<DuckIndexEntry>();
	auto &indexes = duck_index_entry.GetDataTableInfo().indexes.Indexes();

	// get the matching index and serialize its storage info
	for (auto const &index : indexes) {
		if (duck_index_entry.name == index->name) {
			SerializeIndexToWAL(serializer, index);
			break;
		}
	}

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
void WriteAheadLog::WriteSetTable(string &schema, string &table) {
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
void WriteAheadLog::WriteAlter(const AlterInfo &info) {
	WriteAheadLogSerializer serializer(*this, WALType::ALTER_INFO);
	serializer.WriteProperty(101, "info", &info);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	if (skip_writing) {
		return;
	}

	// write an empty entry
	WriteAheadLogSerializer serializer(*this, WALType::WAL_FLUSH);
	serializer.End();

	// flushes all changes made to the WAL to disk
	writer->Sync();
}

} // namespace duckdb
