#include "main/appender.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/exception.hpp"
#include "main/connection.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

Appender::Appender(DuckDB &database, string schema_name, string table_name)
    : db(database), context(database), table_entry(nullptr), column(0) {
	// begin the transaction
	context.transaction.BeginTransaction();

	try {
		table_entry = db.catalog->GetTable(context.transaction.ActiveTransaction(), schema_name, table_name);
	} catch (...) {
		if (context.transaction.IsAutoCommit()) {
			context.transaction.Rollback();
		}
		throw;
	}
	// get the table entry
	auto types = table_entry->GetTypes();
	chunk.Initialize(types);
}

Appender::~Appender() {
	Rollback();
}

void Appender::CheckAppend(TypeId type) {
	if (column >= chunk.column_count) {
		throw Exception("Too many appends for chunk!");
	}
	assert(type == TypeId::INVALID || chunk.data[column].type == type);
}

void Appender::BeginRow() {
	column = 0;
}

void Appender::EndRow() {
	// check that all rows have been appended to
	if (column != chunk.column_count) {
		throw Exception("Call to Appender::EndRow() without all rows having been "
		                "appended to!");
	}
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		Flush();
	}
}

void Appender::AppendTinyInt(int8_t value) {
	CheckAppend(TypeId::TINYINT);
	auto &col = chunk.data[column++];
	((int8_t *)col.data)[col.count++] = value;
}

void Appender::AppendSmallInt(int16_t value) {
	CheckAppend(TypeId::SMALLINT);
	auto &col = chunk.data[column++];
	((int16_t *)col.data)[col.count++] = value;
}

void Appender::AppendInteger(int value) {
	CheckAppend(TypeId::INTEGER);
	auto &col = chunk.data[column++];
	((int32_t *)col.data)[col.count++] = value;
}

void Appender::AppendBigInt(int64_t value) {
	CheckAppend(TypeId::BIGINT);
	auto &col = chunk.data[column++];
	((int64_t *)col.data)[col.count++] = value;
}

void Appender::AppendString(const char *value) {
	CheckAppend(TypeId::VARCHAR);
	AppendValue(Value(value));
}

void Appender::AppendDouble(double value) {
	CheckAppend(TypeId::DOUBLE);
	auto &col = chunk.data[column++];
	((double *)col.data)[col.count++] = value;
}

void Appender::AppendValue(Value value) {
	CheckAppend();
	chunk.data[column].SetValue(chunk.data[column].count++, value);
	column++;
}

void Appender::Flush() {
	assert(table_entry);
	table_entry->storage->Append(*table_entry, context, chunk);
	chunk.Reset();
	column = 0;
}

void Appender::Commit() {
	if (table_entry) {
		Flush();
		context.transaction.Commit();
		table_entry = nullptr;
	}
}

void Appender::Rollback() {
	if (table_entry) {
		Flush();
		context.transaction.Rollback();
		table_entry = nullptr;
	}
}
