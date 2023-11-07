#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include <cstring>

namespace duckdb {

WriteAheadLog::WriteAheadLog(AttachedDatabase &database, const string &path) : skip_writing(false), database(database) {
	wal_path = path;
	writer = make_uniq<BufferedFileWriter>(FileSystem::Get(database), path.c_str(),
	                                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                           FileFlags::FILE_FLAGS_APPEND);
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
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCheckpoint(MetaBlockPointer meta_block) {
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CHECKPOINT);
	serializer.WriteProperty(101, "meta_block", meta_block);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(const TableCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TABLE);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(const TableCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TABLE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(const SchemaCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(const SequenceCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_SEQUENCE);
	serializer.WriteProperty(101, "sequence", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropSequence(const SequenceCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_SEQUENCE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteSequenceValue(const SequenceCatalogEntry &entry, SequenceValue val) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::SEQUENCE_VALUE);
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
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_MACRO);
	serializer.WriteProperty(101, "macro", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropMacro(const ScalarMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteCreateTableMacro(const TableMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TABLE_MACRO);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropTableMacro(const TableMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TABLE_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_INDEX);
	serializer.WriteProperty(101, "index", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropIndex(const IndexCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_INDEX);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(const TypeCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TYPE);
	serializer.WriteProperty(101, "type", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropType(const TypeCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TYPE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(const ViewCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_VIEW);
	serializer.WriteProperty(101, "view", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropView(const ViewCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_VIEW);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(const SchemaCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(string &schema, string &table) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::USE_TABLE);
	serializer.WriteProperty(101, "schema", schema);
	serializer.WriteProperty(102, "table", table);
	serializer.End();
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::INSERT_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DELETE_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::UPDATE_TUPLE);
	serializer.WriteProperty(101, "column_indexes", column_indexes);
	serializer.WriteProperty(102, "chunk", chunk);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Write ALTER Statement
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteAlter(const AlterInfo &info) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::ALTER_INFO);
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

	BinarySerializer serializer(*writer);
	serializer.Begin();
	// write an empty entry
	serializer.WriteProperty(100, "wal_type", WALType::WAL_FLUSH);
	serializer.End();

	// flushes all changes made to the WAL to disk
	writer->Sync();
}

} // namespace duckdb
