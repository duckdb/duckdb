#include "duckdb.hpp"

#include "duckdb/common/arrow.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ArrowScanFunctionData : public TableFunctionData {
	DuckDBArrowTable *table;
	idx_t chunk_idx = 0;
	idx_t chunk_offset = 0;
};

static unique_ptr<FunctionData> arrow_scan_bind(ClientContext &context, vector<Value> &inputs,
                                                unordered_map<string, Value> &named_parameters,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	auto res = make_unique<ArrowScanFunctionData>();
	res->table = (DuckDBArrowTable *)inputs[0].GetValue<uintptr_t>();
	if (!res->table) {
		throw InvalidInputException("arrow_scan: NULL pointer passed");
	}
	if (!res->table->schema.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}
	if (res->table->schema.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}

	for (idx_t col_idx = 0; col_idx < (idx_t)res->table->schema.n_children; col_idx++) {
		auto &schema = *res->table->schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		auto format = string(schema.format);
		if (format == "b") {
			return_types.push_back(LogicalType::BOOLEAN);
		} else if (format == "c") {
			return_types.push_back(LogicalType::TINYINT);
		} else if (format == "s") {
			return_types.push_back(LogicalType::SMALLINT);
		} else if (format == "i") {
			return_types.push_back(LogicalType::INTEGER);
		} else if (format == "l") {
			return_types.push_back(LogicalType::BIGINT);
		} else if (format == "f") {
			return_types.push_back(LogicalType::FLOAT);
		} else if (format == "g") {
			return_types.push_back(LogicalType::DOUBLE);
		} else if (format == "d:38,0") { // decimal128
			return_types.push_back(LogicalType::HUGEINT);
		} else if (format == "u") {
			return_types.push_back(LogicalType::VARCHAR);
		} else if (format == "tsn:") {
			return_types.push_back(LogicalType::TIMESTAMP);
		} else {
			throw NotImplementedException("Unsupported Arrow type %s", format);
		}
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}

	return move(res);
}

static void arrow_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output,
                                FunctionData *dataptr) {
	auto &data = *((ArrowScanFunctionData *)dataptr);

	// have we run out of data on the current chunk? move to next one
	if (data.chunk_offset >= (idx_t)data.table->chunks[data.chunk_idx].length) {
		data.chunk_idx++;
		data.chunk_offset = 0;
	}

	// have we run out of chunks? we done
	if (data.chunk_idx >= data.table->chunks.size()) {
		return;
	}
	auto &current_arrow_chunk = data.table->chunks[data.chunk_idx];

	if (!current_arrow_chunk.release) {
		throw InvalidInputException("arrow_scan: released array passed");
	}
	if ((idx_t)current_arrow_chunk.n_children != output.column_count()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}

	output.SetCardinality(
	    std::min((int64_t)STANDARD_VECTOR_SIZE, (int64_t)(current_arrow_chunk.length - data.chunk_offset)));

	for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
		auto &array = *current_arrow_chunk.children[col_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != current_arrow_chunk.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		if (array.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		if (array.null_count != 0 && array.buffers[0]) {
			auto &nullmask = FlatVector::Nullmask(output.data[col_idx]);

			auto bit_offset = data.chunk_offset + array.offset;
			auto n_bitmask_bytes = (output.size() + 8 - 1) / 8;

			if (bit_offset % 8 == 0) {
				// just memcpy nullmask
				memcpy(&nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
			} else {
				// need to re-align nullmask :/
				bitset<STANDARD_VECTOR_SIZE + 8> temp_nullmask;
				memcpy(&temp_nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);

				temp_nullmask >>= (bit_offset % 8); // why this has to be a right shift is a mystery to me
				memcpy(&nullmask, &temp_nullmask, n_bitmask_bytes);
			}
			nullmask.flip(); // arrow uses inverse nullmask logic
		}

		switch (output.data[col_idx].type.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
			FlatVector::SetData(output.data[col_idx],
			                    (data_ptr_t)array.buffers[1] + GetTypeIdSize(output.data[col_idx].type.InternalType()) *
			                                                       (data.chunk_offset + array.offset));
			break;

		case LogicalTypeId::VARCHAR: {
			auto offsets = (uint32_t *)array.buffers[1] + array.offset + data.chunk_offset;
			auto cdata = (char *)array.buffers[2];

			for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
				if (FlatVector::Nullmask(output.data[col_idx])[row_idx]) {
					continue;
				}
				auto cptr = cdata + offsets[row_idx];
				auto str_len = offsets[row_idx + 1] - offsets[row_idx];

				auto utf_type = Utf8Proc::Analyze(cptr, str_len);
				switch (utf_type) {
				case UnicodeType::ASCII:
					FlatVector::GetData<string_t>(output.data[col_idx])[row_idx] =
					    StringVector::AddString(output.data[col_idx], cptr, str_len);
					break;
				case UnicodeType::UNICODE:
					// this regrettably copies to normalize
					FlatVector::GetData<string_t>(output.data[col_idx])[row_idx] =
					    StringVector::AddString(output.data[col_idx], Utf8Proc::Normalize(string(cptr, str_len)));

					break;
				case UnicodeType::INVALID:
					throw runtime_error("Invalid UTF8 string encoding");
				}
			}

			break;
		} // TODO timestamps in duckdb are subject to change
		case LogicalTypeId::TIMESTAMP: {
			auto src_ptr = (uint64_t *)array.buffers[1] + data.chunk_offset;
			auto tgt_ptr = (timestamp_t *)FlatVector::GetData(output.data[col_idx]);

			for (idx_t row = 0; row < output.size(); row++) {
				auto source_idx = data.chunk_offset + row;

				auto ms = src_ptr[source_idx] / 1000000; // nanoseconds
				auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
				date_t date = Date::EpochToDate(ms / 1000);
				dtime_t time = (dtime_t)(ms % ms_per_day);
				tgt_ptr[row] = Timestamp::FromDatetime(date, time);
			}
			break;
		}
		default:
			throw runtime_error("Unsupported type " + output.data[col_idx].type.ToString());
		}
	}
	output.Verify();
	data.chunk_offset += output.size();
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet arrow("arrow_scan");

	arrow.AddFunction(TableFunction({LogicalType::POINTER}, arrow_scan_bind, arrow_scan_function));
	set.AddFunction(arrow);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
