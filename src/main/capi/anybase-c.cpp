#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/transaction/timestamp_manager.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/data_table.hpp"

using duckdb::ArrowConverter;
using duckdb::ArrowAppender;
using duckdb::ArrowResultWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::ErrorData;

uint64_t duckdb_get_hlc_timestamp() {
	return duckdb::TimestampManager::GetHLCTimestamp();
}

void duckdb_set_hlc_timestamp(uint64_t ts) {
	duckdb::TimestampManager::SetHLCTimestamp(ts);
}

uint64_t duckdb_get_snapshot_id(duckdb_connection connection)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  return conn->GetSnapshotId();
}

uint64_t duckdb_checkpoint_and_get_snapshot_id(duckdb_connection connection)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  return conn->CheckpointAndGetSnapshotId();
}

duckdb_state duckdb_create_snapshot(duckdb_connection connection, duckdb_result *out_result, char **out_snapshot_file_name)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  auto result = conn->CreateSnapshot();
  if (! result.second) {
    return DuckDBError;
  }
  *out_snapshot_file_name = (char *)duckdb_malloc(result.first.length() + 1);
  memcpy(*out_snapshot_file_name, result.first.c_str(), result.first.length() + 1);
  return DuckDBTranslateResult(std::move(result.second), out_result);
}

void duckdb_remove_snapshot(duckdb_connection connection, const char *snapshot_file_name)
{
  Connection *conn = reinterpret_cast<Connection *>(connection);
  conn->RemoveSnapshot(snapshot_file_name);
}

duckdb_state duckdb_result_to_arrow(duckdb_result result, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return DuckDBSuccess;
	}

	if (!result.internal_data) {
		return DuckDBError;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return DuckDBError;
	}
	if (result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// This API is only supported for materialized query results
		return DuckDBError;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	auto &collection = materialized.Collection();

	auto chunk_count = collection.ChunkCount();
	std::unordered_map<idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_type_cast;
	ArrowAppender appender(collection.Types(), chunk_count * STANDARD_VECTOR_SIZE, materialized.client_properties, extension_type_cast);

	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
		collection.FetchChunk(i, *chunk);
		appender.Append(*chunk, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

duckdb_state duckdb_data_chunks_to_arrow_array(duckdb_connection  connection, duckdb_data_chunk *chunks, idx_t number_of_chunks, duckdb_arrow_array *out_array) {
	if (!chunks || number_of_chunks == 0 || !out_array)  {
		return DuckDBSuccess;
	}

	auto options = ((Connection *)connection)->context->GetClientProperties();
	auto chunk_count = number_of_chunks;
	auto first_chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[0]);
	auto types = first_chunk->GetTypes();
	std::unordered_map<idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_type_cast;
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);
	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[i]);
		appender.Append(*chunk, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

duckdb_state duckdb_data_chunk_column_to_arrow_array(duckdb_connection connection, duckdb_data_chunk *chunks, idx_t number_of_chunks, idx_t column_index, duckdb_arrow_array *out_array) {
	if (!chunks || number_of_chunks == 0 || !out_array)  {
		return DuckDBSuccess;
	}

	auto options = ((Connection *)connection)->context->GetClientProperties();
	auto chunk_count = number_of_chunks;
	auto first_chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[0]);
	auto types = duckdb::vector<duckdb::LogicalType>{first_chunk->GetTypes()[column_index]};
	std::unordered_map<idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> extension_type_cast;
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options, extension_type_cast);
	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = reinterpret_cast<duckdb::DataChunk *>(chunks[i]);
		appender.Append(*chunk, 0, column_index, 0, chunk->size(), chunk->size());
	}

	auto *p_array = reinterpret_cast<ArrowArray *>(*out_array);
	*p_array = appender.Finalize();

	return DuckDBSuccess;
}

duckdb_data_chunk duckdb_create_data_chunk_copy(duckdb_data_chunk *chunk) {
	if (!chunk) {
		return nullptr;
	}
	
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(*chunk);

	auto new_chunk = new duckdb::DataChunk();
	new_chunk->Initialize(duckdb::Allocator::DefaultAllocator(), dchunk->GetTypes());

	dchunk->Copy(*new_chunk);

	return reinterpret_cast<duckdb_data_chunk>(new_chunk);
}

idx_t duckdb_get_table_version(const duckdb_connection connection, const char *schema, const char *table, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetTableVersion(schema, table);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

idx_t duckdb_get_column_version(const duckdb_connection connection, const char *schema, const char *table, const char *column, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetColumnVersion(schema, table, column);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

idx_t duckdb_estimated_row_count(const duckdb_connection connection, const char *catalog, const char *schema, const char *table, char **error) {
	auto *ddbConnection = reinterpret_cast<Connection *>(connection);

	try {
		return ddbConnection->context->GetTotalRows(catalog, schema, table);
	} catch (std::exception &ex) {
		if (error) {
			ErrorData parsed_error(ex);
			*error = strdup(parsed_error.Message().c_str());
		}
		return 0;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		return 0;
	} // LCOV_EXCL_STOP
}

void duckdb_set_cdc_callback(duckdb_database db, duckdb_change_data_capture_callback_t function) {
	auto wrapper = reinterpret_cast<duckdb::DatabaseWrapper *>(db);
	auto &config = duckdb::DBConfig::GetConfig(*wrapper->database->instance);
	config.change_data_capture.function = function;
}

