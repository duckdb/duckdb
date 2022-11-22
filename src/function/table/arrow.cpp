#include "duckdb/common/arrow/arrow.hpp"

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

LogicalType ArrowTableFunction::GetArrowLogicalType(
    ArrowSchema &schema, std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx) {
	auto format = string(schema.format);
	if (arrow_convert_data.find(col_idx) == arrow_convert_data.end()) {
		arrow_convert_data[col_idx] = make_unique<ArrowConvertData>();
	}
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
		return LogicalType::DECIMAL(width, scale);
	} else if (format == "u") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::NORMAL, 0);
		return LogicalType::VARCHAR;
	} else if (format == "U") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::SUPER_SIZE, 0);
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
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::DAYS);
		return LogicalType::DATE;
	} else if (format == "tdm") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MILLISECONDS);
		return LogicalType::DATE;
	} else if (format == "tts") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::SECONDS);
		return LogicalType::TIME;
	} else if (format == "ttm") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MILLISECONDS);
		return LogicalType::TIME;
	} else if (format == "ttu") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MICROSECONDS);
		return LogicalType::TIME;
	} else if (format == "ttn") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::NANOSECONDS);
		return LogicalType::TIME;
	} else if (format == "tDs") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::SECONDS);
		return LogicalType::INTERVAL;
	} else if (format == "tDm") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MILLISECONDS);
		return LogicalType::INTERVAL;
	} else if (format == "tDu") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MICROSECONDS);
		return LogicalType::INTERVAL;
	} else if (format == "tDn") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::NANOSECONDS);
		return LogicalType::INTERVAL;
	} else if (format == "tiD") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::DAYS);
		return LogicalType::INTERVAL;
	} else if (format == "tiM") {
		arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MONTHS);
		return LogicalType::INTERVAL;
	} else if (format == "+l") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::NORMAL, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(child_type);
	} else if (format == "+L") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::SUPER_SIZE, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(child_type);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::FIXED_SIZE, fixed_size);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(move(child_type));
	} else if (format == "+s") {
		child_list_t<LogicalType> child_types;
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			auto child_type = GetArrowLogicalType(*schema.children[type_idx], arrow_convert_data, col_idx);
			child_types.push_back({schema.children[type_idx]->name, child_type});
		}
		return LogicalType::STRUCT(move(child_types));

	} else if (format == "+m") {
		child_list_t<LogicalType> child_types;
		//! First type will be struct, so we skip it
		auto &struct_schema = *schema.children[0];
		for (idx_t type_idx = 0; type_idx < (idx_t)struct_schema.n_children; type_idx++) {
			//! The other types must be added on lists
			auto child_type = GetArrowLogicalType(*struct_schema.children[type_idx], arrow_convert_data, col_idx);

			auto list_type = LogicalType::LIST(child_type);
			child_types.push_back({struct_schema.children[type_idx]->name, list_type});
		}
		return LogicalType::MAP(move(child_types));
	} else if (format == "z") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::NORMAL, 0);
		return LogicalType::BLOB;
	} else if (format == "Z") {
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::SUPER_SIZE, 0);
		return LogicalType::BLOB;
	} else if (format[0] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		arrow_convert_data[col_idx]->variable_sz_type.emplace_back(ArrowVariableSizeType::FIXED_SIZE, fixed_size);
		return LogicalType::BLOB;
	} else if (format[0] == 't' && format[1] == 's') {
		// Timestamp with Timezone
		if (format[2] == 'n') {
			arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::NANOSECONDS);
		} else if (format[2] == 'u') {
			arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MICROSECONDS);
		} else if (format[2] == 'm') {
			arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::MILLISECONDS);
		} else if (format[2] == 's') {
			arrow_convert_data[col_idx]->date_time_precision.emplace_back(ArrowDateTimeType::SECONDS);
		} else {
			throw NotImplementedException(" Timestamptz precision of not accepted");
		}
		// TODO right now we just get the UTC value. We probably want to support this properly in the future
		return LogicalType::TIMESTAMP_TZ;
	} else {
		throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
	}
}

void ArrowTableFunction::RenameArrowColumns(vector<string> &names) {
	unordered_map<string, idx_t> name_map;
	for (auto &column_name : names) {
		// put it all lower_case
		auto low_column_name = StringUtil::Lower(column_name);
		if (name_map.find(low_column_name) == name_map.end()) {
			// Name does not exist yet
			name_map[low_column_name]++;
		} else {
			// Name already exists, we add _x where x is the repetition number
			string new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
			auto new_column_name_low = StringUtil::Lower(new_column_name);
			while (name_map.find(new_column_name_low) != name_map.end()) {
				// This name is already here due to a previous definition
				name_map[low_column_name]++;
				new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
				new_column_name_low = StringUtil::Lower(new_column_name);
			}
			column_name = new_column_name;
			name_map[new_column_name_low]++;
		}
	}
}

unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto stream_factory_ptr = input.inputs[0].GetPointer();
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer();

	auto res = make_unique<ArrowScanFunctionData>(stream_factory_produce, stream_factory_ptr);

	auto &data = *res;
	stream_factory_get_schema(stream_factory_ptr, data.schema_root);
	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
		auto &schema = *data.schema_root.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			res->arrow_convert_data[col_idx] =
			    make_unique<ArrowConvertData>(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
			return_types.emplace_back(GetArrowLogicalType(*schema.dictionary, res->arrow_convert_data, col_idx));
		} else {
			return_types.emplace_back(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
		}
		auto format = string(schema.format);
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	RenameArrowColumns(names);
	res->all_types = return_types;
	return move(res);
}

unique_ptr<ArrowArrayStreamWrapper> ProduceArrowScan(const ArrowScanFunctionData &function,
                                                     const vector<column_t> &column_ids, TableFilterSet *filters) {
	//! Generate Projection Pushdown Vector
	ArrowStreamParameters parameters;
	D_ASSERT(!column_ids.empty());
	for (idx_t idx = 0; idx < column_ids.size(); idx++) {
		auto col_idx = column_ids[idx];
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID) {
			auto &schema = *function.schema_root.arrow_schema.children[col_idx];
			parameters.projected_columns.projection_map[idx] = schema.name;
			parameters.projected_columns.columns.emplace_back(schema.name);
		}
	}
	parameters.filters = filters;
	return function.scanner_producer(function.stream_factory_ptr, parameters);
}

idx_t ArrowTableFunction::ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	return context.db->NumberOfThreads();
}

bool ArrowTableFunction::ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                                    ArrowScanLocalState &state, ArrowScanGlobalState &parallel_state) {
	lock_guard<mutex> parallel_lock(parallel_state.main_mutex);
	if (parallel_state.done) {
		return false;
	}
	state.chunk_offset = 0;
	state.batch_index = ++parallel_state.batch_index;

	auto current_chunk = parallel_state.stream->GetNextChunk();
	while (current_chunk->arrow_array.length == 0 && current_chunk->arrow_array.release) {
		current_chunk = parallel_state.stream->GetNextChunk();
	}
	state.chunk = move(current_chunk);
	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		parallel_state.done = true;
		return false;
	}
	return true;
}

unique_ptr<GlobalTableFunctionState> ArrowTableFunction::ArrowScanInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto &bind_data = (const ArrowScanFunctionData &)*input.bind_data;
	auto result = make_unique<ArrowScanGlobalState>();
	result->stream = ProduceArrowScan(bind_data, input.column_ids, input.filters);
	result->max_threads = ArrowScanMaxThreads(context, input.bind_data);
	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(bind_data.all_types[col_idx]);
			}
		}
	}
	return move(result);
}

unique_ptr<LocalTableFunctionState> ArrowTableFunction::ArrowScanInitLocal(ExecutionContext &context,
                                                                           TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *global_state_p) {
	auto &global_state = (ArrowScanGlobalState &)*global_state_p;
	auto current_chunk = make_unique<ArrowArrayWrapper>();
	auto result = make_unique<ArrowScanLocalState>(move(current_chunk));
	result->column_ids = input.column_ids;
	result->filters = input.filters;
	if (input.CanRemoveFilterColumns()) {
		auto &asgs = (ArrowScanGlobalState &)*global_state_p;
		result->all_columns.Initialize(context.client, asgs.scanned_types);
	}
	if (!ArrowScanParallelStateNext(context.client, input.bind_data, *result, global_state)) {
		return nullptr;
	}
	return move(result);
}

void ArrowTableFunction::ArrowScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = (ArrowScanFunctionData &)*data_p.bind_data;
	auto &state = (ArrowScanLocalState &)*data_p.local_state;
	auto &global_state = (ArrowScanGlobalState &)*data_p.global_state;

	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		if (!ArrowScanParallelStateNext(context, data_p.bind_data, state, global_state)) {
			return;
		}
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	if (global_state.CanRemoveFilterColumns()) {
		state.all_columns.Reset();
		state.all_columns.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_convert_data, state.all_columns, data.lines_read - output_size);
		output.ReferenceColumns(state.all_columns, global_state.projection_ids);
	} else {
		output.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_convert_data, output, data.lines_read - output_size);
	}

	output.Verify();
	state.chunk_offset += output.size();
}

unique_ptr<NodeStatistics> ArrowTableFunction::ArrowScanCardinality(ClientContext &context, const FunctionData *data) {
	return make_unique<NodeStatistics>();
}

idx_t ArrowTableFunction::ArrowGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                             LocalTableFunctionState *local_state,
                                             GlobalTableFunctionState *global_state) {
	auto &state = (ArrowScanLocalState &)*local_state;
	return state.batch_index;
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction arrow("arrow_scan", {LogicalType::POINTER, LogicalType::POINTER, LogicalType::POINTER},
	                    ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow.cardinality = ArrowScanCardinality;
	arrow.get_batch_index = ArrowGetBatchIndex;
	arrow.projection_pushdown = true;
	arrow.filter_pushdown = true;
	arrow.filter_prune = true;
	set.AddFunction(arrow);

	TableFunction arrow_dumb("arrow_scan_dumb", {LogicalType::POINTER, LogicalType::POINTER, LogicalType::POINTER},
	                    ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow_dumb.cardinality = ArrowScanCardinality;
	arrow_dumb.get_batch_index = ArrowGetBatchIndex;
	arrow_dumb.projection_pushdown = false;
	arrow_dumb.filter_pushdown = false;
	arrow_dumb.filter_prune = false;
	set.AddFunction(arrow_dumb);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
