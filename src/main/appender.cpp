#include "duckdb/main/appender.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

Appender::Appender(Connection &con, string schema_name, string table_name) : con(con), column(0) {
	description = con.TableInfo(schema_name, table_name);
	if (!description) {
		// table could not be found
		throw CatalogException(
		    StringUtil::Format("Table \"%s.%s\" could not be found", schema_name.c_str(), table_name.c_str()));
	} else {
		vector<TypeId> types;
		for (auto &column : description->columns) {
			types.push_back(GetInternalType(column.type));
		}
		chunk.Initialize(types);
		con.context->RegisterAppender(this);
	}
}

Appender::Appender(Connection &con, string table_name) : Appender(con, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	if (invalidated_msg.empty()) {
		Close();
	}
}

void Appender::CheckInvalidated() {
	if (!invalidated_msg.empty()) {
		throw Exception("Invalid appender: " + invalidated_msg);
	}
}

bool Appender::CheckAppend(TypeId type) {
	if (column >= chunk.column_count) {
		throw Exception("Too many appends for chunk!");
	}
	return chunk.data[column].type == type;
}

void Appender::BeginRow() {
	CheckInvalidated();
	column = 0;
}

void Appender::EndRow() {
	CheckInvalidated();
	// check that all rows have been appended to
	if (column != chunk.column_count) {
		throw Exception("Call to Appender::EndRow() without all rows having been "
		                "appended to!");
	}
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		Flush();
	}
}

template <> void Appender::Append(bool value) {
	if (!CheckAppend(TypeId::BOOLEAN)) {
		AppendValue(Value::BOOLEAN(value));
		return;
	}
	// fast path: correct type
	auto &col = chunk.data[column++];
	((bool *)col.data)[col.count++] = value;
}

template <> void Appender::Append(int8_t value) {
	if (!CheckAppend(TypeId::TINYINT)) {
		AppendValue(Value::TINYINT(value));
		return;
	}
	// fast path: correct type
	auto &col = chunk.data[column++];
	((int8_t *)col.data)[col.count++] = value;
}

template <> void Appender::Append(int16_t value) {
	if (!CheckAppend(TypeId::SMALLINT)) {
		AppendValue(Value::SMALLINT(value));
		return;
	}
	auto &col = chunk.data[column++];
	((int16_t *)col.data)[col.count++] = value;
}

template <> void Appender::Append(int32_t value) {
	if (!CheckAppend(TypeId::INTEGER)) {
		AppendValue(Value::INTEGER(value));
		return;
	}
	auto &col = chunk.data[column++];
	((int32_t *)col.data)[col.count++] = value;
}

template <> void Appender::Append(int64_t value) {
	if (!CheckAppend(TypeId::BIGINT)) {
		AppendValue(Value::BIGINT(value));
		return;
	}
	auto &col = chunk.data[column++];
	((int64_t *)col.data)[col.count++] = value;
}

template <> void Appender::Append(const char *value) {
	if (!CheckAppend(TypeId::VARCHAR)) {
		AppendValue(Value(value));
		return;
	}
	Append<Value>(Value(value));
}

template <> void Appender::Append(double value) {
	if (!CheckAppend(TypeId::DOUBLE)) {
		AppendValue(Value::DOUBLE(value));
		return;
	}
	auto &col = chunk.data[column++];
	((double *)col.data)[col.count++] = value;
}

template <> void Appender::Append(Value value) {
	if (column >= chunk.column_count) {
		throw Exception("Too many appends for chunk!");
	}
	AppendValue(move(value));
}

template <> void Appender::Append(nullptr_t value) {
	if (column >= chunk.column_count) {
		throw Exception("Too many appends for chunk!");
	}
	auto &col = chunk.data[column++];
	col.nullmask[col.count++] = true;
}

void Appender::AppendValue(Value value) {
	chunk.data[column].SetValue(chunk.data[column].count++, value);
	column++;
}

void Appender::Flush() {
	if (chunk.size() == 0) {
		return;
	}
	CheckInvalidated();

	try {
		con.Append(*description, chunk);
	} catch (Exception &ex) {
		con.context->RemoveAppender(this);
		Invalidate(ex.what());
		throw ex;
	}
	chunk.Reset();
	column = 0;
}

void Appender::Close() {
	Flush();
	con.context->RemoveAppender(this);
	Invalidate("The appender has been closed!");
}

void Appender::Invalidate(string msg) {
	if (!invalidated_msg.empty()) {
		return;
	}
	assert(!msg.empty());
	invalidated_msg = msg;
}
