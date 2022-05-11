#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

template <class T>
void WriteData(duckdb_column *column, ChunkCollection &source, idx_t col) {
	idx_t row = 0;
	auto target = (T *)column->__deprecated_data;
	for (auto &chunk : source.Chunks()) {
		auto source = FlatVector::GetData<T>(chunk->data[col]);
		auto &mask = FlatVector::Validity(chunk->data[col]);

		for (idx_t k = 0; k < chunk->size(); k++, row++) {
			if (!mask.RowIsValid(k)) {
				continue;
			}
			target[row] = source[k];
		}
	}
}

duckdb_state deprecated_duckdb_translate_column(MaterializedQueryResult &result, duckdb_column *column, idx_t col) {
	idx_t row_count = result.collection.Count();
	column->__deprecated_nullmask = (bool *)duckdb_malloc(sizeof(bool) * result.collection.Count());
	column->__deprecated_data = duckdb_malloc(GetCTypeSize(column->__deprecated_type) * row_count);
	if (!column->__deprecated_nullmask || !column->__deprecated_data) { // LCOV_EXCL_START
		// malloc failure
		return DuckDBError;
	} // LCOV_EXCL_STOP

	// first convert the nullmask
	idx_t row = 0;
	for (auto &chunk : result.collection.Chunks()) {
		for (idx_t k = 0; k < chunk->size(); k++) {
			column->__deprecated_nullmask[row++] = FlatVector::IsNull(chunk->data[col], k);
		}
	}
	// then write the data
	switch (result.types[col].id()) {
	case LogicalTypeId::BOOLEAN:
		WriteData<bool>(column, result.collection, col);
		break;
	case LogicalTypeId::TINYINT:
		WriteData<int8_t>(column, result.collection, col);
		break;
	case LogicalTypeId::SMALLINT:
		WriteData<int16_t>(column, result.collection, col);
		break;
	case LogicalTypeId::INTEGER:
		WriteData<int32_t>(column, result.collection, col);
		break;
	case LogicalTypeId::BIGINT:
		WriteData<int64_t>(column, result.collection, col);
		break;
	case LogicalTypeId::UTINYINT:
		WriteData<uint8_t>(column, result.collection, col);
		break;
	case LogicalTypeId::USMALLINT:
		WriteData<uint16_t>(column, result.collection, col);
		break;
	case LogicalTypeId::UINTEGER:
		WriteData<uint32_t>(column, result.collection, col);
		break;
	case LogicalTypeId::UBIGINT:
		WriteData<uint64_t>(column, result.collection, col);
		break;
	case LogicalTypeId::FLOAT:
		WriteData<float>(column, result.collection, col);
		break;
	case LogicalTypeId::DOUBLE:
		WriteData<double>(column, result.collection, col);
		break;
	case LogicalTypeId::DATE:
		WriteData<date_t>(column, result.collection, col);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		WriteData<dtime_t>(column, result.collection, col);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		WriteData<timestamp_t>(column, result.collection, col);
		break;
	case LogicalTypeId::VARCHAR: {
		idx_t row = 0;
		auto target = (const char **)column->__deprecated_data;
		for (auto &chunk : result.collection.Chunks()) {
			auto source = FlatVector::GetData<string_t>(chunk->data[col]);
			for (idx_t k = 0; k < chunk->size(); k++) {
				if (!FlatVector::IsNull(chunk->data[col], k)) {
					target[row] = (char *)duckdb_malloc(source[k].GetSize() + 1);
					assert(target[row]);
					memcpy((void *)target[row], source[k].GetDataUnsafe(), source[k].GetSize());
					auto write_arr = (char *)target[row];
					write_arr[source[k].GetSize()] = '\0';
				} else {
					target[row] = nullptr;
				}
				row++;
			}
		}
		break;
	}
	case LogicalTypeId::BLOB: {
		idx_t row = 0;
		auto target = (duckdb_blob *)column->__deprecated_data;
		for (auto &chunk : result.collection.Chunks()) {
			auto source = FlatVector::GetData<string_t>(chunk->data[col]);
			for (idx_t k = 0; k < chunk->size(); k++) {
				if (!FlatVector::IsNull(chunk->data[col], k)) {
					target[row].data = (char *)duckdb_malloc(source[k].GetSize());
					target[row].size = source[k].GetSize();
					assert(target[row].data);
					memcpy((void *)target[row].data, source[k].GetDataUnsafe(), source[k].GetSize());
				} else {
					target[row].data = nullptr;
					target[row].size = 0;
				}
				row++;
			}
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		idx_t row = 0;
		auto target = (timestamp_t *)column->__deprecated_data;
		for (auto &chunk : result.collection.Chunks()) {
			auto source = FlatVector::GetData<timestamp_t>(chunk->data[col]);

			for (idx_t k = 0; k < chunk->size(); k++) {
				if (!FlatVector::IsNull(chunk->data[col], k)) {
					if (result.types[col].id() == LogicalTypeId::TIMESTAMP_NS) {
						target[row] = Timestamp::FromEpochNanoSeconds(source[k].value);
					} else if (result.types[col].id() == LogicalTypeId::TIMESTAMP_MS) {
						target[row] = Timestamp::FromEpochMs(source[k].value);
					} else {
						D_ASSERT(result.types[col].id() == LogicalTypeId::TIMESTAMP_SEC);
						target[row] = Timestamp::FromEpochSeconds(source[k].value);
					}
				}
				row++;
			}
		}
		break;
	}
	case LogicalTypeId::HUGEINT: {
		idx_t row = 0;
		auto target = (duckdb_hugeint *)column->__deprecated_data;
		for (auto &chunk : result.collection.Chunks()) {
			auto source = FlatVector::GetData<hugeint_t>(chunk->data[col]);
			for (idx_t k = 0; k < chunk->size(); k++) {
				if (!FlatVector::IsNull(chunk->data[col], k)) {
					target[row].lower = source[k].lower;
					target[row].upper = source[k].upper;
				}
				row++;
			}
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		idx_t row = 0;
		auto target = (duckdb_interval *)column->__deprecated_data;
		for (auto &chunk : result.collection.Chunks()) {
			auto source = FlatVector::GetData<interval_t>(chunk->data[col]);
			for (idx_t k = 0; k < chunk->size(); k++) {
				if (!FlatVector::IsNull(chunk->data[col], k)) {
					target[row].days = source[k].days;
					target[row].months = source[k].months;
					target[row].micros = source[k].micros;
				}
				row++;
			}
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		// get data
		idx_t row = 0;
		auto target = (hugeint_t *)column->__deprecated_data;
		switch (result.types[col].InternalType()) {
		case PhysicalType::INT16: {
			for (auto &chunk : result.collection.Chunks()) {
				auto source = FlatVector::GetData<int16_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].lower = source[k];
						target[row].upper = 0;
					}
					row++;
				}
			}
			break;
		}
		case PhysicalType::INT32: {
			for (auto &chunk : result.collection.Chunks()) {
				auto source = FlatVector::GetData<int32_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].lower = source[k];
						target[row].upper = 0;
					}
					row++;
				}
			}
			break;
		}
		case PhysicalType::INT64: {
			for (auto &chunk : result.collection.Chunks()) {
				auto source = FlatVector::GetData<int64_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].lower = source[k];
						target[row].upper = 0;
					}
					row++;
				}
			}
			break;
		}
		case PhysicalType::INT128: {
			for (auto &chunk : result.collection.Chunks()) {
				auto source = FlatVector::GetData<hugeint_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].lower = source[k].lower;
						target[row].upper = source[k].upper;
					}
					row++;
				}
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" +
			                         TypeIdToString(result.types[col].InternalType()));
		}
		break;
	}
	default: // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

duckdb_state duckdb_translate_result(unique_ptr<QueryResult> result_p, duckdb_result *out) {
	auto &result = *result_p;
	D_ASSERT(result_p);
	if (!out) {
		// no result to write to, only return the status
		return result.success ? DuckDBSuccess : DuckDBError;
	}

	memset(out, 0, sizeof(duckdb_result));

	// initialize the result_data object
	auto result_data = new DuckDBResultData();
	result_data->result = move(result_p);
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_NONE;
	out->internal_data = result_data;

	if (!result.success) {
		// write the error message
		out->__deprecated_error_message = (char *)result.error.c_str();
		return DuckDBError;
	}
	// copy the data
	// first write the meta data
	out->__deprecated_column_count = result.ColumnCount();
	out->__deprecated_rows_changed = 0;
	return DuckDBSuccess;
}

bool deprecated_materialize_result(duckdb_result *result) {
	if (!result) {
		return false;
	}
	auto result_data = (duckdb::DuckDBResultData *)result->internal_data;
	if (!result_data->result->success) {
		return false;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		// already materialized into deprecated result format
		return true;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED) {
		// already used as a new result set
		return false;
	}
	// materialize as deprecated result set
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED;
	auto column_count = result_data->result->ColumnCount();
	result->__deprecated_columns = (duckdb_column *)duckdb_malloc(sizeof(duckdb_column) * column_count);
	if (!result->__deprecated_columns) { // LCOV_EXCL_START
		// malloc failure
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (result_data->result->type == QueryResultType::STREAM_RESULT) {
		// if we are dealing with a stream result, convert it to a materialized result first
		auto &stream_result = (StreamQueryResult &)*result_data->result;
		result_data->result = stream_result.Materialize();
	}
	D_ASSERT(result_data->result->type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = (MaterializedQueryResult &)*result_data->result;

	// convert the result to a materialized result
	// zero initialize the columns (so we can cleanly delete it in case a malloc fails)
	memset(result->__deprecated_columns, 0, sizeof(duckdb_column) * column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result->__deprecated_columns[i].__deprecated_type = ConvertCPPTypeToC(result_data->result->types[i]);
		result->__deprecated_columns[i].__deprecated_name = (char *)result_data->result->names[i].c_str();
	}
	result->__deprecated_row_count = materialized.collection.Count();
	if (result->__deprecated_row_count > 0 &&
	    materialized.properties.return_type == StatementReturnType::CHANGED_ROWS) {
		// update total changes
		auto row_changes = materialized.GetValue(0, 0);
		if (!row_changes.IsNull() && row_changes.TryCastAs(LogicalType::BIGINT)) {
			result->__deprecated_rows_changed = row_changes.GetValue<int64_t>();
		}
	}
	// now write the data
	for (idx_t col = 0; col < column_count; col++) {
		auto state = deprecated_duckdb_translate_column(materialized, &result->__deprecated_columns[col], col);
		if (state != DuckDBSuccess) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

static void DuckdbDestroyColumn(duckdb_column column, idx_t count) {
	if (column.__deprecated_data) {
		if (column.__deprecated_type == DUCKDB_TYPE_VARCHAR) {
			// varchar, delete individual strings
			auto data = (char **)column.__deprecated_data;
			for (idx_t i = 0; i < count; i++) {
				if (data[i]) {
					duckdb_free(data[i]);
				}
			}
		} else if (column.__deprecated_type == DUCKDB_TYPE_BLOB) {
			// blob, delete individual blobs
			auto data = (duckdb_blob *)column.__deprecated_data;
			for (idx_t i = 0; i < count; i++) {
				if (data[i].data) {
					duckdb_free((void *)data[i].data);
				}
			}
		}
		duckdb_free(column.__deprecated_data);
	}
	if (column.__deprecated_nullmask) {
		duckdb_free(column.__deprecated_nullmask);
	}
}

void duckdb_destroy_result(duckdb_result *result) {
	if (result->__deprecated_columns) {
		for (idx_t i = 0; i < result->__deprecated_column_count; i++) {
			DuckdbDestroyColumn(result->__deprecated_columns[i], result->__deprecated_row_count);
		}
		duckdb_free(result->__deprecated_columns);
	}
	if (result->internal_data) {
		auto result_data = (duckdb::DuckDBResultData *)result->internal_data;
		delete result_data;
	}
	memset(result, 0, sizeof(duckdb_result));
}

const char *duckdb_column_name(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	return result_data.result->names[col].c_str();
}

duckdb_type duckdb_column_type(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	return duckdb::ConvertCPPTypeToC(result_data.result->types[col]);
}

duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	return new duckdb::LogicalType(result_data.result->types[col]);
}

idx_t duckdb_column_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	return result_data.result->ColumnCount();
}

idx_t duckdb_row_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	auto &materialized = (duckdb::MaterializedQueryResult &)*result_data.result;
	return materialized.collection.Count();
}

idx_t duckdb_rows_changed(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return 0;
	}
	return result->__deprecated_rows_changed;
}

void *duckdb_column_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_data;
}

bool *duckdb_nullmask_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_nullmask;
}

const char *duckdb_result_error(duckdb_result *result) {
	if (!result) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result->internal_data);
	return result_data.result->success ? nullptr : result_data.result->error.c_str();
}

idx_t duckdb_result_chunk_count(duckdb_result result) {
	if (!result.internal_data) {
		return 0;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return 0;
	}
	D_ASSERT(result_data.result->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = (duckdb::MaterializedQueryResult &)*result_data.result;
	return materialized.collection.ChunkCount();
}

duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_idx) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	auto &materialized = (duckdb::MaterializedQueryResult &)*result_data.result;
	if (chunk_idx >= materialized.collection.ChunkCount()) {
		return nullptr;
	}
	auto chunk = duckdb::make_unique<duckdb::DataChunk>();
	chunk->InitializeEmpty(materialized.collection.Types());
	chunk->Reference(*materialized.collection.Chunks()[chunk_idx]);
	return chunk.release();
}
