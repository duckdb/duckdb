#include "storage/write_ahead_log.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/view_catalog_entry.hpp"
#include "common/serializer/buffered_deserializer.hpp"
#include "common/serializer/buffered_serializer.hpp"
#include "common/file_system.hpp"
#include "main/client_context.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_table_info.hpp"
#include "parser/parsed_data/create_view_info.hpp"
#include "parser/parsed_data/drop_info.hpp"
#include "planner/parsed_data/bound_create_table_info.hpp"
#include "planner/binder.hpp"
#include "storage/data_table.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

WriteAheadLog::~WriteAheadLog() {
	if (wal_file) {
		fclose(wal_file);
	}
}

void WriteAheadLog::Initialize(string &path) {
	wal_file = fopen(path.c_str(), "a+");
	initialized = true;
}

//===--------------------------------------------------------------------===//
// Write Helper Functions
//===--------------------------------------------------------------------===//
template <class T> void WriteAheadLog::Write(T val) {
	if (fwrite(&val, sizeof(T), 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

void WriteAheadLog::WriteData(data_ptr_t dataptr, index_t data_size) {
	if (data_size == 0) {
		return;
	}
	if (fwrite(dataptr, data_size, 1, wal_file) != 1) {
		throw IOException("WAL - Failed to write (%s)!", strerror(errno));
	}
}

void WriteAheadLog::WriteEntry(wal_type_t type, BufferedSerializer &serializer) {
	auto blob = serializer.GetData();

	Write<wal_type_t>(type);
	Write<uint32_t>((uint32_t)blob.size);
	WriteData(blob.data.get(), blob.size);
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(TableCatalogEntry *entry) {
	BufferedSerializer serializer;
	entry->Serialize(serializer);

	WriteEntry(WALEntry::CREATE_TABLE, serializer);
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(TableCatalogEntry *entry) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_TABLE, serializer);
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(SchemaCatalogEntry *entry) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::CREATE_SCHEMA, serializer);
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(SequenceCatalogEntry *entry) {
	BufferedSerializer serializer;
	entry->Serialize(serializer);
	WriteEntry(WALEntry::CREATE_SEQUENCE, serializer);
}

void WriteAheadLog::WriteDropSequence(SequenceCatalogEntry *entry) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_SEQUENCE, serializer);
}

void WriteAheadLog::WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);
	serializer.Write<uint64_t>(val.usage_count);
	serializer.Write<int64_t>(val.counter);

	WriteEntry(WALEntry::SEQUENCE_VALUE, serializer);
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(ViewCatalogEntry *entry) {
	BufferedSerializer serializer;
	entry->Serialize(serializer);
	WriteEntry(WALEntry::CREATE_VIEW, serializer);
}

void WriteAheadLog::WriteDropView(ViewCatalogEntry *entry) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->schema->name);
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_VIEW, serializer);
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(SchemaCatalogEntry *entry) {
	BufferedSerializer serializer;
	serializer.WriteString(entry->name);

	WriteEntry(WALEntry::DROP_SCHEMA, serializer);
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteInsert(string &schema, string &table, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	chunk.Verify();
	if (chunk.sel_vector) {
		throw NotImplementedException("Cannot insert into WAL from chunk with SEL vector");
	}

	BufferedSerializer serializer;
	serializer.WriteString(schema);
	serializer.WriteString(table);
	chunk.Serialize(serializer);

	WriteEntry(WALEntry::INSERT_TUPLE, serializer);
}

//===--------------------------------------------------------------------===//
// QUERY
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteQuery(string &query) {
	BufferedSerializer serializer;
	serializer.WriteString(query);

	WriteEntry(WALEntry::QUERY, serializer);
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	// write an empty entry
	Write<wal_type_t>(WALEntry::WAL_FLUSH);
	Write<uint32_t>(0);
	// flushes all changes made to the WAL to disk
	fflush(wal_file);
	database.file_system->FileSync(wal_file);
}
