#include "duckdb/main/capi_internal.hpp"

using duckdb::ArrowResultWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResult;
using duckdb::QueryResultType;

duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result) {
	Connection *conn = (Connection *)connection;
	auto wrapper = new ArrowResultWrapper();
	wrapper->result = conn->Query(query);
	*out_result = (duckdb_arrow)wrapper;
	return wrapper->result->success ? DuckDBSuccess : DuckDBError;
}

duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema) {
	if (!out_schema) {
		return DuckDBSuccess;
	}
	auto wrapper = (ArrowResultWrapper *)result;
	QueryResult::ToArrowSchema((ArrowSchema *)*out_schema, wrapper->result->types, wrapper->result->names);
	return DuckDBSuccess;
}

duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return DuckDBSuccess;
	}
	auto wrapper = (ArrowResultWrapper *)result;
	auto success = wrapper->result->TryFetch(wrapper->current_chunk, wrapper->result->error);
	if (!success) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (!wrapper->current_chunk || wrapper->current_chunk->size() == 0) {
		return DuckDBSuccess;
	}
	wrapper->current_chunk->ToArrowArray((ArrowArray *)*out_array);
	return DuckDBSuccess;
}

idx_t duckdb_arrow_row_count(duckdb_arrow result) {
	auto wrapper = (ArrowResultWrapper *)result;
	return wrapper->result->collection.Count();
}

idx_t duckdb_arrow_column_count(duckdb_arrow result) {
	auto wrapper = (ArrowResultWrapper *)result;
	return wrapper->result->types.size();
}

idx_t duckdb_arrow_rows_changed(duckdb_arrow result) {
	auto wrapper = (ArrowResultWrapper *)result;
	idx_t rows_changed = 0;
	idx_t row_count = wrapper->result->collection.Count();
	if (row_count > 0 && StatementTypeReturnChanges(wrapper->result->statement_type)) {
		auto row_changes = wrapper->result->GetValue(0, 0);
		if (!row_changes.IsNull() && row_changes.TryCastAs(LogicalType::BIGINT)) {
			rows_changed = row_changes.GetValue<int64_t>();
		}
	}
	return rows_changed;
}

const char *duckdb_query_arrow_error(duckdb_arrow result) {
	auto wrapper = (ArrowResultWrapper *)result;
	return wrapper->result->error.c_str();
}

void duckdb_destroy_arrow(duckdb_arrow *result) {
	if (*result) {
		auto wrapper = (ArrowResultWrapper *)*result;
		delete wrapper;
		*result = nullptr;
	}
}

duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement, duckdb_arrow *out_result) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || !wrapper->statement->success || !out_result) {
		return DuckDBError;
	}
	auto arrow_wrapper = new ArrowResultWrapper();
	auto result = wrapper->statement->Execute(wrapper->values, false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	arrow_wrapper->result = duckdb::unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	*out_result = (duckdb_arrow)arrow_wrapper;
	return arrow_wrapper->result->success ? DuckDBSuccess : DuckDBError;
}
