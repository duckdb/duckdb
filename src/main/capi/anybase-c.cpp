#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/transaction/timestamp_manager.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/common/types.hpp"

using duckdb::ArrowConverter;
using duckdb::ArrowAppender;
using duckdb::ArrowResultWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;

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
	ArrowAppender appender(collection.Types(), chunk_count * STANDARD_VECTOR_SIZE, materialized.client_properties);

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
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options);
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
	ArrowAppender appender(types, chunk_count * STANDARD_VECTOR_SIZE, options);
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

