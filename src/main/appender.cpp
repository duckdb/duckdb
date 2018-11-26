
#include "main/appender.hpp"
#include "common/exception.hpp"
#include "main/connection.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

Appender::Appender(DuckDBConnection &connection, ClientContext &context,
                   TableCatalogEntry *table_entry)
    : connection(connection), context(context), table_entry(table_entry),
      column(0) {
	auto types = table_entry->GetTypes();
	chunk.Initialize(types);
}

void Appender::begin_append_row() {
	column = 0;
}

void Appender::check_append(TypeId type) {
	if (column >= chunk.column_count) {
		throw Exception("Too many appends for chunk!");
	}
	assert(type == TypeId::INVALID || chunk.data[column].type == type);
}

void Appender::append_tinyint(int8_t value) {
	check_append(TypeId::TINYINT);
	auto &col = chunk.data[column++];
	((int8_t *)col.data)[col.count++] = value;
}

void Appender::append_smallint(int16_t value) {
	check_append(TypeId::SMALLINT);
	auto &col = chunk.data[column++];
	((int16_t *)col.data)[col.count++] = value;
}

void Appender::append_int(int value) {
	check_append(TypeId::INTEGER);
	auto &col = chunk.data[column++];
	((int32_t *)col.data)[col.count++] = value;
}

void Appender::append_bigint(int64_t value) {
	check_append(TypeId::BIGINT);
	auto &col = chunk.data[column++];
	((int64_t *)col.data)[col.count++] = value;
}

void Appender::append_string(const char *value) {
	check_append(TypeId::VARCHAR);
	auto &col = chunk.data[column++];
	((const char **)col.data)[col.count++] = value;
}

void Appender::append_double(double value) {
	check_append(TypeId::DECIMAL);
	auto &col = chunk.data[column++];
	((double *)col.data)[col.count++] = value;
}

void Appender::append_value(Value value) {
	check_append();
	chunk.data[column].SetValue(chunk.data[column].count++, value);
	column++;
}

void Appender::end_append_row() {
	// check that all rows have been appended to
	if (column != chunk.column_count) {
		throw Exception("Call to end_append_row() without all rows having been "
		                "appended to!");
	}
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		flush();
	}
}

void Appender::flush() {
	table_entry->storage->Append(*table_entry, context, chunk);
	chunk.Reset();
	column = 0;
}
