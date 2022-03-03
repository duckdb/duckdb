#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/data_chunk.hpp"

duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *ctypes, idx_t column_count) {
	if (!ctypes) {
		return nullptr;
	}
	duckdb::vector<duckdb::LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		auto ltype = (duckdb::LogicalType *)ctypes[i];
		types.push_back(*ltype);
	}

	auto result = new duckdb::DataChunk();
	result->Initialize(types);
	return result;
}

void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk) {
	if (chunk && *chunk) {
		auto dchunk = (duckdb::DataChunk *)*chunk;
		delete dchunk;
		*chunk = nullptr;
	}
}

void duckdb_data_chunk_reset(duckdb_data_chunk chunk) {
	if (!chunk) {
		return;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	dchunk->Reset();
}

idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	return dchunk->ColumnCount();
}

idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	return dchunk->size();
}

void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size) {
	if (!chunk) {
		return;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	dchunk->SetCardinality(size);
}

duckdb_logical_type duckdb_data_chunk_get_column_type(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= duckdb_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	return new duckdb::LogicalType(dchunk->data[col_idx].GetType());
}

void *duckdb_data_chunk_get_data(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= duckdb_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	return duckdb::FlatVector::GetData(dchunk->data[col_idx]);
}

uint64_t *duckdb_data_chunk_get_validity(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= duckdb_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	return duckdb::FlatVector::Validity(dchunk->data[col_idx]).GetData();
}

void duckdb_data_chunk_ensure_validity_writable(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk) {
		return;
	}
	auto dchunk = (duckdb::DataChunk *)chunk;
	auto &validity = duckdb::FlatVector::Validity(dchunk->data[col_idx]);
	validity.EnsureWritable();
}
