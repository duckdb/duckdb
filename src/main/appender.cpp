
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
	((int32_t *)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_smallint(int16_t value) {
	check_append(TypeId::SMALLINT);
	((int32_t *)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_int(int value) {
	check_append(TypeId::INTEGER);
	((int32_t *)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_bigint(int64_t value) {
	check_append(TypeId::BIGINT);
	((int64_t *)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_string(const char *value) {
	check_append(TypeId::VARCHAR);
	((const char **)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_double(double value) {
	check_append(TypeId::DECIMAL);
	((double *)chunk.data[column++].data)[chunk.count] = value;
}

void Appender::append_value(Value value) {
	check_append();
	chunk.data[column].count++;
	chunk.data[column++].SetValue(chunk.count, value);
}

void Appender::end_append_row() {
	// check that all rows have been appended to
	if (column != chunk.column_count) {
		throw Exception("Call to end_append_row() without all rows having been "
		                "appended to!");
	}
	chunk.count++;
	if (chunk.count >= STANDARD_VECTOR_SIZE) {
		flush();
	}
}

void Appender::flush() {
	for (size_t i = 0; i < chunk.column_count; i++) {
		chunk.data[i].count = chunk.count;
	}
	table_entry->storage->Append(context, chunk);
	chunk.Reset();
	column = 0;
}
