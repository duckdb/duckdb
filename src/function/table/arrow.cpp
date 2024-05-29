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
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

static unique_ptr<ArrowType> CreateListType(ArrowSchema &child, ArrowVariableSizeType size_type, bool view) {
	auto child_type = ArrowTableFunction::GetArrowLogicalType(child);

	unique_ptr<ArrowTypeInfo> type_info;
	auto type = LogicalType::LIST(child_type->GetDuckType());
	if (view) {
		type_info = ArrowListInfo::ListView(std::move(child_type), size_type);
	} else {
		type_info = ArrowListInfo::List(std::move(child_type), size_type);
	}
	return make_uniq<ArrowType>(type, std::move(type_info));
}

static unique_ptr<ArrowType> GetArrowLogicalTypeNoDictionary(ArrowSchema &schema) {
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
		auto extra_info = StringUtil::Split(format, ':');
		if (extra_info.size() != 2) {
			throw InvalidInputException(
			    "Decimal format of Arrow object is incomplete, it is missing the scale and width. Current format: %s",
			    format);
		}
		auto parameters = StringUtil::Split(extra_info[1], ",");
		// Parameters must always be 2 or 3 values (i.e., width, scale and an optional bit-width)
		if (parameters.size() != 2 && parameters.size() != 3) {
			throw InvalidInputException(
			    "Decimal format of Arrow object is incomplete, it is missing the scale or width. Current format: %s",
			    format);
		}
		uint64_t width = std::stoull(parameters[0]);
		uint64_t scale = std::stoull(parameters[1]);
		uint64_t bitwidth = 128;
		if (parameters.size() == 3) {
			// We have a bit-width defined
			bitwidth = std::stoull(parameters[2]);
		}
		if (width > 38 || bitwidth > 128) {
			throw NotImplementedException("Unsupported Internal Arrow Type for Decimal %s", format);
		}
		return make_uniq<ArrowType>(LogicalType::DECIMAL(NumericCast<uint8_t>(width), NumericCast<uint8_t>(scale)));
	} else if (format == "u") {
		return make_uniq<ArrowType>(LogicalType::VARCHAR, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
	} else if (format == "U") {
		return make_uniq<ArrowType>(LogicalType::VARCHAR,
		                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
	} else if (format == "vu") {
		return make_uniq<ArrowType>(LogicalType::VARCHAR, make_uniq<ArrowStringInfo>(ArrowVariableSizeType::VIEW));
	} else if (format == "tsn:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_NS);
	} else if (format == "tsu:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP);
	} else if (format == "tsm:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_MS);
	} else if (format == "tss:") {
		return make_uniq<ArrowType>(LogicalTypeId::TIMESTAMP_SEC);
	} else if (format == "tdD") {
		return make_uniq<ArrowType>(LogicalType::DATE, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::DAYS));
	} else if (format == "tdm") {
		return make_uniq<ArrowType>(LogicalType::DATE, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MILLISECONDS));
	} else if (format == "tts") {
		return make_uniq<ArrowType>(LogicalType::TIME, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::SECONDS));
	} else if (format == "ttm") {
		return make_uniq<ArrowType>(LogicalType::TIME, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MILLISECONDS));
	} else if (format == "ttu") {
		return make_uniq<ArrowType>(LogicalType::TIME, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MICROSECONDS));
	} else if (format == "ttn") {
		return make_uniq<ArrowType>(LogicalType::TIME, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::NANOSECONDS));
	} else if (format == "tDs") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::SECONDS));
	} else if (format == "tDm") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL,
		                            make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MILLISECONDS));
	} else if (format == "tDu") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL,
		                            make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MICROSECONDS));
	} else if (format == "tDn") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL,
		                            make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::NANOSECONDS));
	} else if (format == "tiD") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::DAYS));
	} else if (format == "tiM") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL, make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MONTHS));
	} else if (format == "tin") {
		return make_uniq<ArrowType>(LogicalType::INTERVAL,
		                            make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MONTH_DAY_NANO));
	} else if (format == "+l") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::NORMAL, false);
	} else if (format == "+L") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::SUPER_SIZE, false);
	} else if (format == "+vl") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::NORMAL, true);
	} else if (format == "+vL") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::SUPER_SIZE, true);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		auto fixed_size = NumericCast<idx_t>(std::stoi(parameters));
		auto child_type = ArrowTableFunction::GetArrowLogicalType(*schema.children[0]);

		auto array_type = LogicalType::ARRAY(child_type->GetDuckType(), fixed_size);
		auto type_info = make_uniq<ArrowArrayInfo>(std::move(child_type), fixed_size);
		return make_uniq<ArrowType>(array_type, std::move(type_info));
	} else if (format == "+s") {
		child_list_t<LogicalType> child_types;
		vector<unique_ptr<ArrowType>> children;
		if (schema.n_children == 0) {
			throw InvalidInputException(
			    "Attempted to convert a STRUCT with no fields to DuckDB which is not supported");
		}
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*schema.children[type_idx]));
			child_types.emplace_back(schema.children[type_idx]->name, children.back()->GetDuckType());
		}
		auto type_info = make_uniq<ArrowStructInfo>(std::move(children));
		auto struct_type = make_uniq<ArrowType>(LogicalType::STRUCT(std::move(child_types)), std::move(type_info));
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
		if (schema.n_children == 0) {
			throw InvalidInputException("Attempted to convert a UNION with no fields to DuckDB which is not supported");
		}
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			auto type = schema.children[type_idx];

			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*type));
			members.emplace_back(type->name, children.back()->GetDuckType());
		}

		auto type_info = make_uniq<ArrowStructInfo>(std::move(children));
		auto union_type = make_uniq<ArrowType>(LogicalType::UNION(members), std::move(type_info));
		return union_type;
	} else if (format == "+r") {
		child_list_t<LogicalType> members;
		vector<unique_ptr<ArrowType>> children;
		idx_t n_children = idx_t(schema.n_children);
		D_ASSERT(n_children == 2);
		D_ASSERT(string(schema.children[0]->name) == "run_ends");
		D_ASSERT(string(schema.children[1]->name) == "values");
		for (idx_t i = 0; i < n_children; i++) {
			auto type = schema.children[i];
			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*type));
			members.emplace_back(type->name, children.back()->GetDuckType());
		}

		auto type_info = make_uniq<ArrowStructInfo>(std::move(children));
		auto struct_type = make_uniq<ArrowType>(LogicalType::STRUCT(members), std::move(type_info));
		struct_type->SetRunEndEncoded();
		return struct_type;
	} else if (format == "+m") {
		auto &arrow_struct_type = *schema.children[0];
		D_ASSERT(arrow_struct_type.n_children == 2);
		auto key_type = ArrowTableFunction::GetArrowLogicalType(*arrow_struct_type.children[0]);
		auto value_type = ArrowTableFunction::GetArrowLogicalType(*arrow_struct_type.children[1]);
		child_list_t<LogicalType> key_value;
		key_value.emplace_back(std::make_pair("key", key_type->GetDuckType()));
		key_value.emplace_back(std::make_pair("value", value_type->GetDuckType()));

		auto map_type = LogicalType::MAP(key_type->GetDuckType(), value_type->GetDuckType());
		vector<unique_ptr<ArrowType>> children;
		children.reserve(2);
		children.push_back(std::move(key_type));
		children.push_back(std::move(value_type));
		auto inner_struct = make_uniq<ArrowType>(LogicalType::STRUCT(std::move(key_value)),
		                                         make_uniq<ArrowStructInfo>(std::move(children)));
		auto map_type_info = ArrowListInfo::List(std::move(inner_struct), ArrowVariableSizeType::NORMAL);
		return make_uniq<ArrowType>(map_type, std::move(map_type_info));
	} else if (format == "z") {
		auto type_info = make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL);
		return make_uniq<ArrowType>(LogicalType::BLOB, std::move(type_info));
	} else if (format == "Z") {
		auto type_info = make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE);
		return make_uniq<ArrowType>(LogicalType::BLOB, std::move(type_info));
	} else if (format[0] == 'w') {
		string parameters = format.substr(format.find(':') + 1);
		auto fixed_size = NumericCast<idx_t>(std::stoi(parameters));
		auto type_info = make_uniq<ArrowStringInfo>(fixed_size);
		return make_uniq<ArrowType>(LogicalType::BLOB, std::move(type_info));
	} else if (format[0] == 't' && format[1] == 's') {
		// Timestamp with Timezone
		// TODO right now we just get the UTC value. We probably want to support this properly in the future
		unique_ptr<ArrowTypeInfo> type_info;
		if (format[2] == 'n') {
			type_info = make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::NANOSECONDS);
		} else if (format[2] == 'u') {
			type_info = make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MICROSECONDS);
		} else if (format[2] == 'm') {
			type_info = make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::MILLISECONDS);
		} else if (format[2] == 's') {
			type_info = make_uniq<ArrowDateTimeInfo>(ArrowDateTimeType::SECONDS);
		} else {
			throw NotImplementedException(" Timestamptz precision of not accepted");
		}
		return make_uniq<ArrowType>(LogicalType::TIMESTAMP_TZ, std::move(type_info));
	} else {
		throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
	}
}

unique_ptr<ArrowType> ArrowTableFunction::GetArrowLogicalType(ArrowSchema &schema) {
	auto arrow_type = GetArrowLogicalTypeNoDictionary(schema);
	if (schema.dictionary) {
		auto dictionary = GetArrowLogicalType(*schema.dictionary);
		arrow_type->SetDictionary(std::move(dictionary));
	}
	return arrow_type;
}

void ArrowTableFunction::PopulateArrowTableType(ArrowTableType &arrow_table, ArrowSchemaWrapper &schema_p,
                                                vector<string> &names, vector<LogicalType> &return_types) {
	for (idx_t col_idx = 0; col_idx < (idx_t)schema_p.arrow_schema.n_children; col_idx++) {
		auto &schema = *schema_p.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		auto arrow_type = GetArrowLogicalType(schema);
		return_types.emplace_back(arrow_type->GetDuckType(true));
		arrow_table.AddColumn(col_idx, std::move(arrow_type));
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
}

unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
		throw BinderException("arrow_scan: pointers cannot be null");
	}
	auto &ref = input.ref;

	shared_ptr<DependencyItem> dependency;
	if (ref.external_dependency) {
		// This was created during the replacement scan for Python (see python_replacement_scan.cpp)
		// this object is the owning reference to 'stream_factory_ptr' and has to be kept alive.
		dependency = ref.external_dependency->GetDependency("replacement_cache");
		D_ASSERT(dependency);
	}

	auto stream_factory_ptr = input.inputs[0].GetPointer();
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();       // NOLINT
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer(); // NOLINT

	auto res = make_uniq<ArrowScanFunctionData>(stream_factory_produce, stream_factory_ptr, std::move(dependency));

	auto &data = *res;
	stream_factory_get_schema(reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr), data.schema_root.arrow_schema);
	PopulateArrowTableType(res->arrow_table, data.schema_root, names, return_types);
	QueryResult::DeduplicateColumns(names);
	res->all_types = return_types;
	if (return_types.empty()) {
		throw InvalidInputException("Provided table/dataframe must have at least one column");
	}
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
			parameters.projected_columns.filter_to_col[idx] = col_idx;
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
	state.Reset();
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
	auto output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
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
