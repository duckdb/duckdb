#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

template <class T>
void WriteData(duckdb_result *out, ChunkCollection &source, idx_t col) {
	idx_t row = 0;
	auto target = (T *)out->__deprecated_columns[col].__deprecated_data;
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

duckdb_state duckdb_translate_result(MaterializedQueryResult *result, duckdb_result *out) {
	D_ASSERT(result);
	if (!out) {
		// no result to write to, only return the status
		return result->success ? DuckDBSuccess : DuckDBError;
	}
	memset(out, 0, sizeof(duckdb_result));
	if (!result->success) {
		// write the error message
		out->__deprecated_error_message = strdup(result->error.c_str());
		return DuckDBError;
	}
	// copy the data
	// first write the meta data
	out->__deprecated_column_count = result->types.size();
	out->__deprecated_row_count = result->collection.Count();
	out->__deprecated_rows_changed = 0;
	if (out->__deprecated_row_count > 0 && StatementTypeReturnChanges(result->statement_type)) {
		// update total changes
		auto row_changes = result->GetValue(0, 0);
		if (!row_changes.IsNull() && row_changes.TryCastAs(LogicalType::BIGINT)) {
			out->__deprecated_rows_changed = row_changes.GetValue<int64_t>();
		}
	}
	out->__deprecated_columns = (duckdb_column *)duckdb_malloc(sizeof(duckdb_column) * out->__deprecated_column_count);
	if (!out->__deprecated_columns) { // LCOV_EXCL_START
		// malloc failure
		return DuckDBError;
	} // LCOV_EXCL_STOP

	// zero initialize the columns (so we can cleanly delete it in case a malloc fails)
	memset(out->__deprecated_columns, 0, sizeof(duckdb_column) * out->__deprecated_column_count);
	for (idx_t i = 0; i < out->__deprecated_column_count; i++) {
		auto column_data = new DuckDBColumnData();
		column_data->type = result->types[i];
		out->__deprecated_columns[i].internal_data = column_data;
		out->__deprecated_columns[i].__deprecated_type = ConvertCPPTypeToC(result->types[i]);
		out->__deprecated_columns[i].__deprecated_name = strdup(result->names[i].c_str());
		out->__deprecated_columns[i].__deprecated_nullmask =
		    (bool *)duckdb_malloc(sizeof(bool) * out->__deprecated_row_count);
		out->__deprecated_columns[i].__deprecated_data =
		    duckdb_malloc(GetCTypeSize(out->__deprecated_columns[i].__deprecated_type) * out->__deprecated_row_count);
		if (!out->__deprecated_columns[i].__deprecated_nullmask || !out->__deprecated_columns[i].__deprecated_name ||
		    !out->__deprecated_columns[i].__deprecated_data) { // LCOV_EXCL_START
			// malloc failure
			return DuckDBError;
		} // LCOV_EXCL_STOP
	}
	// now write the data
	for (idx_t col = 0; col < out->__deprecated_column_count; col++) {
		// first set the nullmask
		idx_t row = 0;
		for (auto &chunk : result->collection.Chunks()) {
			for (idx_t k = 0; k < chunk->size(); k++) {
				out->__deprecated_columns[col].__deprecated_nullmask[row++] = FlatVector::IsNull(chunk->data[col], k);
			}
		}
		// then write the data
		switch (result->types[col].id()) {
		case LogicalTypeId::BOOLEAN:
			WriteData<bool>(out, result->collection, col);
			break;
		case LogicalTypeId::TINYINT:
			WriteData<int8_t>(out, result->collection, col);
			break;
		case LogicalTypeId::SMALLINT:
			WriteData<int16_t>(out, result->collection, col);
			break;
		case LogicalTypeId::INTEGER:
			WriteData<int32_t>(out, result->collection, col);
			break;
		case LogicalTypeId::BIGINT:
			WriteData<int64_t>(out, result->collection, col);
			break;
		case LogicalTypeId::UTINYINT:
			WriteData<uint8_t>(out, result->collection, col);
			break;
		case LogicalTypeId::USMALLINT:
			WriteData<uint16_t>(out, result->collection, col);
			break;
		case LogicalTypeId::UINTEGER:
			WriteData<uint32_t>(out, result->collection, col);
			break;
		case LogicalTypeId::UBIGINT:
			WriteData<uint64_t>(out, result->collection, col);
			break;
		case LogicalTypeId::FLOAT:
			WriteData<float>(out, result->collection, col);
			break;
		case LogicalTypeId::DOUBLE:
			WriteData<double>(out, result->collection, col);
			break;
		case LogicalTypeId::DATE:
			WriteData<date_t>(out, result->collection, col);
			break;
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			WriteData<dtime_t>(out, result->collection, col);
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			WriteData<timestamp_t>(out, result->collection, col);
			break;
		case LogicalTypeId::VARCHAR: {
			idx_t row = 0;
			auto target = (const char **)out->__deprecated_columns[col].__deprecated_data;
			for (auto &chunk : result->collection.Chunks()) {
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
			auto target = (duckdb_blob *)out->__deprecated_columns[col].__deprecated_data;
			for (auto &chunk : result->collection.Chunks()) {
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
			auto target = (timestamp_t *)out->__deprecated_columns[col].__deprecated_data;
			for (auto &chunk : result->collection.Chunks()) {
				auto source = FlatVector::GetData<timestamp_t>(chunk->data[col]);

				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						if (result->types[col].id() == LogicalTypeId::TIMESTAMP_NS) {
							target[row] = Timestamp::FromEpochNanoSeconds(source[k].value);
						} else if (result->types[col].id() == LogicalTypeId::TIMESTAMP_MS) {
							target[row] = Timestamp::FromEpochMs(source[k].value);
						} else {
							D_ASSERT(result->types[col].id() == LogicalTypeId::TIMESTAMP_SEC);
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
			auto target = (duckdb_hugeint *)out->__deprecated_columns[col].__deprecated_data;
			for (auto &chunk : result->collection.Chunks()) {
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
			auto target = (duckdb_interval *)out->__deprecated_columns[col].__deprecated_data;
			for (auto &chunk : result->collection.Chunks()) {
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
			auto target = (hugeint_t *)out->__deprecated_columns[col].__deprecated_data;
			switch (result->types[col].InternalType()) {
			case PhysicalType::INT16: {
				for (auto &chunk : result->collection.Chunks()) {
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
				for (auto &chunk : result->collection.Chunks()) {
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
				for (auto &chunk : result->collection.Chunks()) {
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
				for (auto &chunk : result->collection.Chunks()) {
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
				                         TypeIdToString(result->types[col].InternalType()));
			}
			break;
		}
		default: // LCOV_EXCL_START
			std::string err_msg = "Unsupported type for C API: " + result->types[col].ToString();
			out->__deprecated_error_message = strdup(err_msg.c_str());
			return DuckDBError;
		} // LCOV_EXCL_STOP
	}
	return DuckDBSuccess;
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
	if (column.__deprecated_name) {
		duckdb_free(column.__deprecated_name);
	}
	if (column.internal_data) {
		auto column_data = (duckdb::DuckDBColumnData *)column.internal_data;
		delete column_data;
	}
}

void duckdb_destroy_result(duckdb_result *result) {
	if (result->__deprecated_error_message) {
		duckdb_free(result->__deprecated_error_message);
	}
	if (result->__deprecated_columns) {
		for (idx_t i = 0; i < result->__deprecated_column_count; i++) {
			DuckdbDestroyColumn(result->__deprecated_columns[i], result->__deprecated_row_count);
		}
		duckdb_free(result->__deprecated_columns);
	}
	memset(result, 0, sizeof(duckdb_result));
}

const char *duckdb_column_name(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_name;
}

duckdb_type duckdb_column_type(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return DUCKDB_TYPE_INVALID;
	}
	return result->__deprecated_columns[col].__deprecated_type;
}

idx_t duckdb_column_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	return result->__deprecated_column_count;
}

idx_t duckdb_row_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	return result->__deprecated_row_count;
}

idx_t duckdb_rows_changed(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	return result->__deprecated_rows_changed;
}

void *duckdb_column_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_data;
}

bool *duckdb_nullmask_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_nullmask;
}

char *duckdb_result_error(duckdb_result *result) {
	if (!result) {
		return nullptr;
	}
	return result->__deprecated_error_message;
}
