#include "duckdb/main/capi/capi_internal.hpp"

using duckdb::Connection;
using duckdb::ErrorData;
using duckdb::TableDescription;
using duckdb::TableDescriptionWrapper;

duckdb_state duckdb_table_description_create(duckdb_connection connection, const char *schema, const char *table,
                                             duckdb_table_description *out) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!connection || !table || !out) {
		return DuckDBError;
	}
	if (schema == nullptr) {
		schema = DEFAULT_SCHEMA;
	}
	auto wrapper = new TableDescriptionWrapper();
	try {
		wrapper->description = conn->TableInfo(schema, table);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		wrapper->error = error.RawMessage();
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown Connection::TableInfo error";
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (!wrapper->description) {
		delete wrapper;
		return DuckDBError;
	}
	*out = (duckdb_table_description)wrapper;
	return DuckDBSuccess;
}

duckdb_state duckdb_table_description_destroy(duckdb_table_description *table) {
	if (!table || !*table) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(*table);
	if (wrapper) {
		delete wrapper;
	}
	*table = nullptr;
	return DuckDBSuccess;
}

const char *duckdb_table_description_error(duckdb_table_description table) {
	if (!table) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(table);
	if (wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

duckdb_state duckdb_column_has_default(duckdb_table_description table_description, idx_t index, bool *out) {
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(table_description);
	if (!wrapper || !out) {
		return DuckDBError;
	}

	auto &table = wrapper->description;
	if (index >= table->columns.size()) {
		return DuckDBError;
	}
	auto &column = table->columns[index];
	*out = column.HasDefaultValue();
	return DuckDBSuccess;
}
