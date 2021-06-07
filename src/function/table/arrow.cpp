#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb.hpp"

#include "duckdb/common/arrow.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/to_string.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/hugeint.hpp"
namespace duckdb {

LogicalType GetArrowLogicalType(ArrowSchema &schema,
                                std::unordered_map<idx_t, vector<std::pair<ArrowListType, idx_t>>> &arrow_lists,
                                idx_t col_idx) {
	auto format = string(schema.format);
	if (format == "n") {
		return LogicalType::SQLNULL;
	} else if (format == "b") {
		return LogicalType::BOOLEAN;
	} else if (format == "c") {
		return LogicalType::TINYINT;
	} else if (format == "s") {
		return LogicalType::SMALLINT;
	} else if (format == "i") {
		return LogicalType::INTEGER;
	} else if (format == "l") {
		return LogicalType::BIGINT;
	} else if (format == "C") {
		return LogicalType::UTINYINT;
	} else if (format == "S") {
		return LogicalType::USMALLINT;
	} else if (format == "I") {
		return LogicalType::UINTEGER;
	} else if (format == "L") {
		return LogicalType::UBIGINT;
	} else if (format == "f") {
		return LogicalType::FLOAT;
	} else if (format == "g") {
		return LogicalType::DOUBLE;
	} else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
		std::string parameters = format.substr(format.find(':'));
		uint8_t width = std::stoi(parameters.substr(1, parameters.find(',')));
		uint8_t scale = std::stoi(parameters.substr(parameters.find(',') + 1));
		if (width > 38) {
			throw NotImplementedException("Unsupported Internal Arrow Type for Decimal %s", format);
		}
		return LogicalType(LogicalTypeId::DECIMAL, width, scale);
	} else if (format == "u") {
		return LogicalType::VARCHAR;
	} else if (format == "tsn:") {
		return LogicalTypeId::TIMESTAMP_NS;
	} else if (format == "tsu:") {
		return LogicalTypeId::TIMESTAMP;
	} else if (format == "tsm:") {
		return LogicalTypeId::TIMESTAMP_MS;
	} else if (format == "tss:") {
		return LogicalTypeId::TIMESTAMP_SEC;
	} else if (format == "tdD") {
		return LogicalType::DATE;
	} else if (format == "ttm") {
		return LogicalType::TIME;
	} else if (format == "+l") {
		arrow_lists[col_idx].emplace_back(ArrowListType::NORMAL, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_lists, col_idx);
		child_list_t<LogicalType> child_types {{"", child_type}};
		return LogicalType(LogicalTypeId::LIST, child_types);
	} else if (format == "+L") {
		arrow_lists[col_idx].emplace_back(ArrowListType::SUPER_SIZE, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_lists, col_idx);
		child_list_t<LogicalType> child_types {{"", child_type}};
		return LogicalType(LogicalTypeId::LIST, child_types);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		arrow_lists[col_idx].emplace_back(ArrowListType::FIXED_SIZE, fixed_size);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_lists, col_idx);
		child_list_t<LogicalType> child_types {{"", child_type}};
		return LogicalType(LogicalTypeId::LIST, child_types);
	} else {
		throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
	}
}

unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBind(ClientContext &context, vector<Value> &inputs,
                                                           unordered_map<string, Value> &named_parameters,
                                                           vector<LogicalType> &input_table_types,
                                                           vector<string> &input_table_names,
                                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto stream_factory_ptr = inputs[0].GetPointer();
	unique_ptr<ArrowArrayStreamWrapper> (*stream_factory_produce)(uintptr_t stream_factory_ptr) =
	    (unique_ptr<ArrowArrayStreamWrapper>(*)(uintptr_t stream_factory_ptr))inputs[1].GetPointer();
	auto rows_per_thread = inputs[2].GetValue<uint64_t>();

	auto res = make_unique<ArrowScanFunctionData>(rows_per_thread);
	auto &data = *res;
	data.stream = stream_factory_produce(stream_factory_ptr);
	if (!data.stream) {
		throw InvalidInputException("arrow_scan: NULL pointer passed");
	}

	data.stream->GetSchema(data.schema_root);

	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
		auto &schema = *data.schema_root.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		auto format = string(schema.format);
		return_types.emplace_back(GetArrowLogicalType(schema, res->original_list_type, col_idx));
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	return move(res);
}

unique_ptr<FunctionOperatorData> ArrowTableFunction::ArrowScanInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   const vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArrayWrapper>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	return move(result);
}

void SetValidityMask(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size) {
	if (array.null_count != 0 && array.buffers[0]) {

		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto &mask = FlatVector::Validity(vector);
		auto bit_offset = scan_state.chunk_offset + array.offset;
		auto n_bitmask_bytes = (size + 8 - 1) / 8;
		mask.EnsureWritable();
		if (bit_offset % 8 == 0) {
			//! just memcpy nullmask
			memcpy((void *)mask.GetData(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
		} else {
			//! need to re-align nullmask
			bitset<STANDARD_VECTOR_SIZE + 8> temp_nullmask;
			memcpy(&temp_nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);

			temp_nullmask >>= (bit_offset % 8); // why this has to be a right shift is a mystery to me
			memcpy((void *)mask.GetData(), (data_ptr_t)&temp_nullmask, n_bitmask_bytes);
		}
	}
}

void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                         std::unordered_map<idx_t, std::vector<std::pair<ArrowListType, idx_t>>> &arrow_lists,
                         idx_t col_idx, idx_t list_col_idx = 0) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value());
		break;
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		FlatVector::SetData(vector, (data_ptr_t)array.buffers[1] + GetTypeIdSize(vector.GetType().InternalType()) *
		                                                               (scan_state.chunk_offset + array.offset));
		break;

	case LogicalTypeId::VARCHAR: {
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		auto cdata = (char *)array.buffers[2];

		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto cptr = cdata + offsets[row_idx];
			auto str_len = offsets[row_idx + 1] - offsets[row_idx];

			auto utf_type = Utf8Proc::Analyze(cptr, str_len);
			if (utf_type == UnicodeType::INVALID) {
				throw std::runtime_error("Invalid UTF8 string encoding");
			}
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, cptr, str_len);
		}

		break;
	}
	case LogicalTypeId::TIME: {
		// convert time from milliseconds to microseconds
		auto src_ptr = (uint32_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
		auto tgt_ptr = (dtime_t *)FlatVector::GetData(vector);
		for (idx_t row = 0; row < size; row++) {
			tgt_ptr[row] = dtime_t(int64_t(src_ptr[row]) * 1000);
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		//! We have to convert from INT128
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT16: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int16_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
				D_ASSERT(result);
			}
			break;
		}
		case PhysicalType::INT32: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int32_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
				D_ASSERT(result);
			}
			break;
		}
		case PhysicalType::INT64: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int64_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
				D_ASSERT(result);
			}
			break;
		}
		case PhysicalType::INT128: {
			FlatVector::SetData(vector, (data_ptr_t)array.buffers[1] + GetTypeIdSize(vector.GetType().InternalType()) *
			                                                               (scan_state.chunk_offset + array.offset));
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" +
			                         TypeIdToString(vector.GetType().InternalType()));
		}
		break;
	}
	case LogicalTypeId::LIST: {
		auto original_type = arrow_lists[col_idx][list_col_idx];
		idx_t list_size = 0;
		ListVector::Initialize(vector);
		auto &child_vector = ListVector::GetEntry(vector);
		SetValidityMask(vector, array, scan_state, size);

		if (original_type.first == ArrowListType::FIXED_SIZE) {
			//! Have to check validity mask before setting this up
			idx_t offset = 0;
			auto list_data = FlatVector::GetData<list_entry_t>(vector);
			for (idx_t i = 0; i < size; i++) {
				auto &le = list_data[i];
				le.offset = offset;
				le.length = original_type.second;
				offset += original_type.second;
			}
			list_size = offset;
		} else if (original_type.first == ArrowListType::NORMAL) {
			auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
			auto list_data = FlatVector::GetData<list_entry_t>(vector);
			for (idx_t i = 0; i < size; i++) {
				auto &le = list_data[i];
				le.offset = offsets[i];
				le.length = offsets[i + 1] - offsets[i];
			}
			list_size = offsets[size];
		} else {
			auto offsets = (uint64_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
			auto list_data = FlatVector::GetData<list_entry_t>(vector);
			for (idx_t i = 0; i < size; i++) {
				auto &le = list_data[i];
				le.offset = offsets[i];
				le.length = offsets[i + 1] - offsets[i];
			}
			list_size = offsets[size];
		}
		ListVector::SetListSize(vector, list_size);
		SetValidityMask(child_vector, *array.children[0], scan_state, list_size);
		ColumnArrowToDuckDB(child_vector, *array.children[0], scan_state, list_size, arrow_lists, col_idx,
		                    list_col_idx + 1);
		break;
	}
	default:
		throw std::runtime_error("Unsupported type " + vector.GetType().ToString());
	}
}

void ArrowTableFunction::ArrowToDuckDB(ArrowScanState &scan_state,
                                       std::unordered_map<idx_t, vector<std::pair<ArrowListType, idx_t>>> &arrow_lists,
                                       DataChunk &output) {
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &array = *scan_state.chunk->arrow_array.children[col_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->arrow_array.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		if (array.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		SetValidityMask(output.data[col_idx], array, scan_state, output.size());
		ColumnArrowToDuckDB(output.data[col_idx], array, scan_state, output.size(), arrow_lists, col_idx);
	}
}

void ArrowTableFunction::ArrowScanFunction(ClientContext &context, const FunctionData *bind_data,
                                           FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	auto &state = (ArrowScanState &)*operator_state;

	//! have we run out of data on the current chunk? move to next one
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		state.chunk_offset = 0;
		state.chunk = data.stream->GetNextChunk();
	}

	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		return;
	}

	if ((idx_t)state.chunk->arrow_array.n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	output.SetCardinality(output_size);
	ArrowToDuckDB(state, data.original_list_type, output);
	output.Verify();
	state.chunk_offset += output.size();
}

void ArrowTableFunction::ArrowScanFunctionParallel(ClientContext &context, const FunctionData *bind_data,
                                                   FunctionOperatorData *operator_state, DataChunk *input,
                                                   DataChunk &output, ParallelState *parallel_state_p) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	auto &state = (ArrowScanState &)*operator_state;
	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		return;
	}
	if ((idx_t)state.chunk->arrow_array.n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	output.SetCardinality(output_size);
	ArrowToDuckDB(state, data.original_list_type, output);
	output.Verify();
	state.chunk_offset += output.size();
}

idx_t ArrowTableFunction::ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	return (bind_data.stream->number_of_rows + bind_data.rows_per_thread - 1) / bind_data.rows_per_thread;
}

unique_ptr<ParallelState> ArrowTableFunction::ArrowScanInitParallelState(ClientContext &context,
                                                                         const FunctionData *bind_data_p) {
	return make_unique<ParallelArrowScanState>();
}

bool ArrowTableFunction::ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                                    FunctionOperatorData *operator_state,
                                                    ParallelState *parallel_state_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	auto &state = (ArrowScanState &)*operator_state;

	state.chunk_offset = 0;
	state.chunk = bind_data.stream->GetNextChunk();
	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		return false;
	}
	return true;
}

unique_ptr<FunctionOperatorData>
ArrowTableFunction::ArrowScanParallelInit(ClientContext &context, const FunctionData *bind_data_p, ParallelState *state,
                                          const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArrayWrapper>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	if (!ArrowScanParallelStateNext(context, bind_data_p, result.get(), state)) {
		return nullptr;
	}
	return move(result);
}

unique_ptr<NodeStatistics> ArrowTableFunction::ArrowScanCardinality(ClientContext &context, const FunctionData *data) {
	auto &bind_data = (ArrowScanFunctionData &)*data;
	return make_unique<NodeStatistics>(bind_data.stream->number_of_rows, bind_data.stream->number_of_rows);
}

int ArrowTableFunction::ArrowProgress(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	if (bind_data.stream->number_of_rows == 0) {
		return 100;
	}
	auto percentage = bind_data.lines_read * 100 / bind_data.stream->number_of_rows;
	return percentage;
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet arrow("arrow_scan");
	arrow.AddFunction(TableFunction({LogicalType::POINTER, LogicalType::POINTER, LogicalType::UBIGINT},
	                                ArrowScanFunction, ArrowScanBind, ArrowScanInit, nullptr, nullptr, nullptr,
	                                ArrowScanCardinality, nullptr, nullptr, ArrowScanMaxThreads,
	                                ArrowScanInitParallelState, ArrowScanFunctionParallel, ArrowScanParallelInit,
	                                ArrowScanParallelStateNext, false, false, ArrowProgress));
	set.AddFunction(arrow);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
