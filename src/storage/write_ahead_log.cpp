#include "duckdb/storage/write_ahead_log.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

WriteAheadLog::WriteAheadLog(DuckDB &database) : initialized(false), database(database) {
}

void WriteAheadLog::Initialize(string &path) {
	writer = make_unique<BufferedFileWriter>(*database.file_system, path.c_str(), true);
	initialized = true;
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(TableCatalogEntry *entry) {
	writer->Write<WALType>(WALType::CREATE_TABLE);
	entry->Serialize(*writer);
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(TableCatalogEntry *entry) {
	writer->Write<WALType>(WALType::DROP_TABLE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(SchemaCatalogEntry *entry) {
	writer->Write<WALType>(WALType::CREATE_SCHEMA);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(SequenceCatalogEntry *entry) {
	writer->Write<WALType>(WALType::CREATE_SEQUENCE);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropSequence(SequenceCatalogEntry *entry) {
	writer->Write<WALType>(WALType::DROP_SEQUENCE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

void WriteAheadLog::WriteSequenceValue(SequenceCatalogEntry *entry, SequenceValue val) {
	writer->Write<WALType>(WALType::SEQUENCE_VALUE);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
	writer->Write<uint64_t>(val.usage_count);
	writer->Write<int64_t>(val.counter);
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(ViewCatalogEntry *entry) {
	writer->Write<WALType>(WALType::CREATE_VIEW);
	entry->Serialize(*writer);
}

void WriteAheadLog::WriteDropView(ViewCatalogEntry *entry) {
	writer->Write<WALType>(WALType::DROP_VIEW);
	writer->WriteString(entry->schema->name);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(SchemaCatalogEntry *entry) {
	writer->Write<WALType>(WALType::DROP_SCHEMA);
	writer->WriteString(entry->name);
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(string &schema, string &table) {
	writer->Write<WALType>(WALType::USE_TABLE);
	writer->WriteString(schema);
	writer->WriteString(table);
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	assert(chunk.size() > 0);
	chunk.Verify();

	writer->Write<WALType>(WALType::INSERT_TUPLE);
	chunk.Serialize(*writer);
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	assert(chunk.size() > 0);
	assert(chunk.column_count == 1 && chunk.data[0].type == ROW_TYPE);
	chunk.Verify();

	writer->Write<WALType>(WALType::DELETE_TUPLE);
	chunk.Serialize(*writer);
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, column_t col_idx) {
	assert(chunk.size() > 0);
	chunk.Verify();

	writer->Write<WALType>(WALType::UPDATE_TUPLE);
	writer->Write<column_t>(col_idx);
	chunk.Serialize(*writer);
}

//===--------------------------------------------------------------------===//
// QUERY
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteQuery(string &query) {
	writer->Write<WALType>(WALType::QUERY);
	writer->WriteString(query);
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	// write an empty entry
	writer->Write<WALType>(WALType::WAL_FLUSH);
	// flushes all changes made to the WAL to disk
	writer->Sync();
}
