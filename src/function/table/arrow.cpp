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
	ArrowArrayStream *stream;
	ArrowSchema schema_root;
	ArrowArray current_chunk_root;
	idx_t chunk_idx = 0;
	idx_t chunk_offset = 0;
	bool is_consumed = false;

	void ReleaseArray() {
		if (current_chunk_root.release) {
			for (idx_t child_idx = 0; child_idx < (idx_t)current_chunk_root.n_children; child_idx++) {
				auto &child = *current_chunk_root.children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			current_chunk_root.release(&current_chunk_root);
		}
	}

	void ReleaseSchema() {
		if (schema_root.release) {
			for (idx_t child_idx = 0; child_idx < (idx_t)schema_root.n_children; child_idx++) {
				auto &child = *schema_root.children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			schema_root.release(&schema_root);
		}
	}

	~ArrowScanFunctionData() {
		ReleaseSchema();
		ReleaseArray();
	}
};

static unique_ptr<FunctionData> arrow_scan_bind(ClientContext &context, vector<Value> &inputs,
                                                unordered_map<string, Value> &named_parameters,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	auto res = make_unique<ArrowScanFunctionData>();
	auto &data = *res;

	data.stream = (ArrowArrayStream *)inputs[0].GetValue<uintptr_t>();
	if (!data.stream) {
		throw InvalidInputException("arrow_scan: NULL pointer passed");
	}

	if (data.stream->get_schema(data.stream, &data.schema_root)) {
		throw InvalidInputException("arrow_scan: get_schema failed(): %s",
		                            string(data.stream->get_last_error(data.stream)));
	}

	if (!data.schema_root.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}

	if (data.schema_root.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}

	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.n_children; col_idx++) {
		auto &schema = *data.schema_root.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		auto format = string(schema.format);
		if (format == "n") {
			return_types.push_back(LogicalType::SQLNULL);
		} else if (format == "b") {
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
		} else if (format == "tdD") {
			return_types.push_back(LogicalType::DATE);
		} else {
			throw NotImplementedException("Unsupported Arrow type %s", format);
		}
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	data.ReleaseSchema();
	return move(res);
}

static unique_ptr<FunctionOperatorData> arrow_scan_init(ClientContext &context, const FunctionData *bind_data,
                                                        vector<column_t> &column_ids,
                                                        unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	if (data.is_consumed) {
		throw NotImplementedException("FIXME: Arrow streams can only be read once");
	}
	data.is_consumed = true;
	return make_unique<FunctionOperatorData>();
}

static void arrow_scan_function(ClientContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	if (!data.stream->release) {
		// no more chunks
		return;
	}

	// have we run out of data on the current chunk? move to next one
	if (data.chunk_offset >= (idx_t)data.current_chunk_root.length) {
		data.chunk_offset = 0;
		data.ReleaseArray();
		if (data.stream->get_next(data.stream, &data.current_chunk_root)) {
			throw InvalidInputException("arrow_scan: get_next failed(): %s",
			                            string(data.stream->get_last_error(data.stream)));
		}
	}

	// have we run out of chunks? we done
	if (!data.current_chunk_root.release) {
		data.stream->release(data.stream);
		return;
	}

	if ((idx_t)data.current_chunk_root.n_children != output.column_count()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}

	output.SetCardinality(
	    std::min((int64_t)STANDARD_VECTOR_SIZE, (int64_t)(data.current_chunk_root.length - data.chunk_offset)));

	for (idx_t col_idx = 0; col_idx < output.column_count(); col_idx++) {
		auto &array = *data.current_chunk_root.children[col_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != data.current_chunk_root.length) {
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
				memcpy(&nullmask, (data_ptr_t)&temp_nullmask, n_bitmask_bytes);
			}
			nullmask.flip(); // arrow uses inverse nullmask logic
		}

		switch (output.data[col_idx].type.id()) {
		case LogicalTypeId::SQLNULL:
			output.data[col_idx].Reference(Value());
			break;
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
				if (utf_type == UnicodeType::INVALID) {
					throw runtime_error("Invalid UTF8 string encoding");
				}
				FlatVector::GetData<string_t>(output.data[col_idx])[row_idx] =
				    StringVector::AddString(output.data[col_idx], cptr, str_len);
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
		case LogicalTypeId::DATE: {
			auto src_ptr = (int32_t *)array.buffers[1] + data.chunk_offset;
			auto tgt_ptr = (date_t *)FlatVector::GetData(output.data[col_idx]);

			for (idx_t row = 0; row < output.size(); row++) {
				auto source_idx = data.chunk_offset + row;
				tgt_ptr[row] = Date::EpochDaysToDate(src_ptr[source_idx]);
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

	arrow.AddFunction(TableFunction({LogicalType::POINTER}, arrow_scan_function, arrow_scan_bind, arrow_scan_init));
	set.AddFunction(arrow);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
