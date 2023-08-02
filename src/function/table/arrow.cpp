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
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

unique_ptr<ArrowType> ArrowTableFunction::GetArrowLogicalType(ArrowSchema &schema) {
	auto format = string(schema.format);
	if (format == "n") {
		return make_uniq<ArrowType>(LogicalType::SQLNULL);
	} else if (format == "b") {
		return make_uniq<ArrowType>(LogicalType::BOOLEAN);
	} else if (format == "c") {
		return make_uniq<ArrowType>(LogicalType::TINYINT);
	} else if (format == "s") {
		return make_uniq<ArrowType>(LogicalType::SMALLINT);
	} else if (format == "i") {
		return make_uniq<ArrowType>(LogicalType::INTEGER);
	} else if (format == "l") {
		return make_uniq<ArrowType>(LogicalType::BIGINT);
	} else if (format == "C") {
		return make_uniq<ArrowType>(LogicalType::UTINYINT);
	} else if (format == "S") {
		return make_uniq<ArrowType>(LogicalType::USMALLINT);
	} else if (format == "I") {
		return make_uniq<ArrowType>(LogicalType::UINTEGER);
	} else if (format == "L") {
		return make_uniq<ArrowType>(LogicalType::UBIGINT);
	} else if (format == "f") {
		return make_uniq<ArrowType>(LogicalType::FLOAT);
	} else if (format == "g") {
		return make_uniq<ArrowType>(LogicalType::DOUBLE);
	} else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
		std::string parameters = format.substr(format.find(':'));
		uint8_t width = std::stoi(parameters.substr(1, parameters.find(',')));
		uint8_t scale = std::stoi(parameters.substr(parameters.find(',') + 1));
		if (width > 38) {
			throw NotImplementedException("Unsupported Internal Arrow Type for Decimal %s", format);
		}
		return make_uniq<ArrowType>(LogicalType::DECIMAL(width, scale));
	} else if (format == "u") {
		return make_uniq<ArrowType>(LogicalType::VARCHAR, ArrowVariableSizeType::NORMAL);
	} else if (format == "U") {
		return make_uniq<ArrowType>(LogicalType::VARCHAR, ArrowVariableSizeType::SUPER_SIZE);
	} else if (format == "tsn:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_NS);
	} else if (format == "tsu:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP);
	} else if (format == "tsm:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_MS);
	} else if (format == "tss:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_SEC);
	} else if (format == "tdD") {
		return make_uniq<ArrowType>(LogicalType::DATE, ArrowDateTimeType::DAYS);
	} else if (format == "tdm") {
		return make_uniq<ArrowType>(LogicalType::DATE, ArrowDateTimeType::MILLISECONDS);
	} else if (format == "tts") {
		return make_uniq<ArrowType>(LogicalType::TIME, ArrowDateTimeType::SECONDS);
	} else if (format == "ttm") {
		return make_uniq<ArrowType>(LogicalType::TIME, ArrowDateTimeType::MILLISECONDS);
	} else if (format == "ttu") {
		return make_uniq<ArrowType>(LogicalType::TIME, ArrowDateTimeType::MICROSECONDS);
	} else if (format == "ttn") {
		return make_uniq<ArrowType>(LogicalType::TIME, ArrowDateTimeType::NANOSECONDS);
	} else if (format == "tDs") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::SECONDS);
	} else if (format == "tDm") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::MILLISECONDS);
	} else if (format == "tDu") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::MICROSECONDS);
	} else if (format == "tDn") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::NANOSECONDS);
	} else if (format == "tiD") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::DAYS);
	} else if (format == "tiM") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::MONTHS);
	} else if (format == "tin") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, ArrowDateTimeType::MONTH_DAY_NANO);
	} else if (format == "+l") {
		auto child_type = GetArrowLogicalType(*schema.children[0]);
		auto list_type =
		    make_uniq<ArrowType>(LogicalType::LIST(child_type->GetDuckType()), ArrowVariableSizeType::NORMAL);
		list_type->AddChild(std::move(child_type));
		return list_type;
	} else if (format == "+L") {
		auto child_type = GetArrowLogicalType(*schema.children[0]);
		auto list_type =
		    make_uniq<ArrowType>(LogicalType::LIST(child_type->GetDuckType()), ArrowVariableSizeType::SUPER_SIZE);
		list_type->AddChild(std::move(child_type));
		return list_type;
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		auto child_type = GetArrowLogicalType(*schema.children[0]);
		auto list_type = make_uniq<ArrowType>(LogicalType::LIST(child_type->GetDuckType()), fixed_size);
		list_type->AddChild(std::move(child_type));
		return list_type;
	} else if (format == "+s") {
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<ArrowType>> children;
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			children.emplace_back(GetArrowLogicalType(*schema.children[type_idx]));
			child_types.emplace_back(schema.children[type_idx]->name, children.back()->GetDuckType());
		}
		auto struct_type = make_uniq<ArrowType>(LogicalType::STRUCT(std::move(child_types)));
		struct_type->AssignChildren(std::move(children));
		return struct_type;
	} else if (format[0] == '+' && format[1] == 'u') {
		if (format[2] != 's') {
			throw NotImplementedException("Unsupported Internal Arrow Type: \"%c\" Union", format[2]);
		}
		D_ASSERT(format[3] == ':');

		std::string prefix = "+us:";
		// TODO: what are these type ids actually for?
		auto type_ids = StringUtil::Split(format.substr(prefix.size()), ',');

		child_list_t<LogicalType> members;
		vector<unique_ptr<ArrowType>> children;
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			auto type = schema.children[type_idx];

			children.emplace_back(GetArrowLogicalType(*type));
			members.emplace_back(type->name, children.back()->GetDuckType());
		}

		auto union_type = make_uniq<ArrowType>(LogicalType::UNION(members));
		union_type->AssignChildren(std::move(children));
		return union_type;
	} else if (format == "+m") {
		auto &arrow_struct_type = *schema.children[0];
		D_ASSERT(arrow_struct_type.n_children == 2);
		auto key_type = GetArrowLogicalType(*arrow_struct_type.children[0]);
		auto value_type = GetArrowLogicalType(*arrow_struct_type.children[1]);
		auto map_type = make_uniq<ArrowType>(LogicalType::MAP(key_type->GetDuckType(), value_type->GetDuckType()),
		                                     ArrowVariableSizeType::NORMAL);
		child_list_t<LogicalType> key_value;
		key_value.emplace_back(std::make_pair("key", key_type->GetDuckType()));
		key_value.emplace_back(std::make_pair("value", value_type->GetDuckType()));

		auto inner_struct =
		    make_uniq<ArrowType>(LogicalType::STRUCT(std::move(key_value)), ArrowVariableSizeType::NORMAL);
		vector<unique_ptr<ArrowType>> children;
		children.reserve(2);
		children.push_back(std::move(key_type));
		children.push_back(std::move(value_type));
		inner_struct->AssignChildren(std::move(children));
		map_type->AddChild(std::move(inner_struct));
		return map_type;
	} else if (format == "z") {
		return make_uniq<ArrowType>(LogicalType::BLOB, ArrowVariableSizeType::NORMAL);
	} else if (format == "Z") {
		return make_uniq<ArrowType>(LogicalType::BLOB, ArrowVariableSizeType::SUPER_SIZE);
	} else if (format[0] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		return make_uniq<ArrowType>(LogicalType::BLOB, fixed_size);
	} else if (format[0] == 't' && format[1] == 's') {
		// Timestamp with Timezone
		// TODO right now we just get the UTC value. We probably want to support this properly in the future
		if (format[2] == 'n') {
			return make_uniq<ArrowType>(LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::NANOSECONDS);
		} else if (format[2] == 'u') {
			return make_uniq<ArrowType>(LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::MICROSECONDS);
		} else if (format[2] == 'm') {
			return make_uniq<ArrowType>(LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::MILLISECONDS);
		} else if (format[2] == 's') {
			return make_uniq<ArrowType>(LogicalType::TIMESTAMP_TZ, ArrowDateTimeType::SECONDS);
		} else {
			throw NotImplementedException(" Timestamptz precision of not accepted");
		}
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
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();       // NOLINT
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer(); // NOLINT

	auto res = make_uniq<ArrowScanFunctionData>(stream_factory_produce, stream_factory_ptr);

	auto &data = *res;
	stream_factory_get_schema(stream_factory_ptr, data.schema_root);
	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
		auto &schema = *data.schema_root.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		auto arrow_type = GetArrowLogicalType(schema);
		if (schema.dictionary) {
			auto logical_type = arrow_type->GetDuckType();
			auto dictionary = GetArrowLogicalType(*schema.dictionary);
			return_types.emplace_back(dictionary->GetDuckType());
			// The dictionary might have different attributes (size type, datetime precision, etc..)
			arrow_type->SetDictionary(std::move(dictionary));
		} else {
			return_types.emplace_back(arrow_type->GetDuckType());
		}
		res->arrow_table.AddColumn(col_idx, std::move(arrow_type));
		auto format = string(schema.format);
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	RenameArrowColumns(names);
	res->all_types = return_types;
	return std::move(res);
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
	state.chunk = std::move(current_chunk);
	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		parallel_state.done = true;
		return false;
	}
	return true;
}

unique_ptr<GlobalTableFunctionState> ArrowTableFunction::ArrowScanInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ArrowScanFunctionData>();
	auto result = make_uniq<ArrowScanGlobalState>();
	result->stream = ProduceArrowScan(bind_data, input.column_ids, input.filters.get());
	result->max_threads = ArrowScanMaxThreads(context, input.bind_data.get());
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
	return std::move(result);
}

unique_ptr<LocalTableFunctionState>
ArrowTableFunction::ArrowScanInitLocalInternal(ClientContext &context, TableFunctionInitInput &input,
                                               GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<ArrowScanGlobalState>();
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto result = make_uniq<ArrowScanLocalState>(std::move(current_chunk));
	result->column_ids = input.column_ids;
	result->filters = input.filters.get();
	if (input.CanRemoveFilterColumns()) {
		auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
		result->all_columns.Initialize(context, asgs.scanned_types);
	}
	if (!ArrowScanParallelStateNext(context, input.bind_data.get(), *result, global_state)) {
		return nullptr;
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> ArrowTableFunction::ArrowScanInitLocal(ExecutionContext &context,
                                                                           TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *global_state_p) {
	return ArrowScanInitLocalInternal(context.client, input, global_state_p);
}

void ArrowTableFunction::ArrowScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
	auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
	auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		if (!ArrowScanParallelStateNext(context, data_p.bind_data.get(), state, global_state)) {
			return;
		}
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	if (global_state.CanRemoveFilterColumns()) {
		state.all_columns.Reset();
		state.all_columns.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns, data.lines_read - output_size);
		output.ReferenceColumns(state.all_columns, global_state.projection_ids);
	} else {
		output.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_table.GetColumns(), output, data.lines_read - output_size);
	}

	output.Verify();
	state.chunk_offset += output.size();
}

unique_ptr<NodeStatistics> ArrowTableFunction::ArrowScanCardinality(ClientContext &context, const FunctionData *data) {
	return make_uniq<NodeStatistics>();
}

idx_t ArrowTableFunction::ArrowGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                             LocalTableFunctionState *local_state,
                                             GlobalTableFunctionState *global_state) {
	auto &state = local_state->Cast<ArrowScanLocalState>();
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
