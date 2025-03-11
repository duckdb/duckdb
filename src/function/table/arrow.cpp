#include "duckdb/common/arrow/arrow.hpp"

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"

namespace duckdb {

void ArrowTableFunction::PopulateArrowTableType(DBConfig &config, ArrowTableType &arrow_table,
                                                const ArrowSchemaWrapper &schema_p, vector<string> &names,
                                                vector<LogicalType> &return_types) {
	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(schema_p.arrow_schema.n_children); col_idx++) {
		auto &schema = *schema_p.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		auto arrow_type = ArrowType::GetArrowLogicalType(config, schema);
		return_types.emplace_back(arrow_type->GetDuckType(true));
		arrow_table.AddColumn(col_idx, std::move(arrow_type));
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
}

unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBindDumb(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto bind_data = ArrowScanBind(context, input, return_types, names);
	auto &arrow_bind_data = bind_data->Cast<ArrowScanFunctionData>();
	arrow_bind_data.projection_pushdown_enabled = false;
	return bind_data;
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
	PopulateArrowTableType(DBConfig::GetConfig(context), res->arrow_table, data.schema_root, names, return_types);
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
	auto &arrow_types = function.arrow_table.GetColumns();
	for (idx_t idx = 0; idx < column_ids.size(); idx++) {
		auto col_idx = column_ids[idx];
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID) {
			auto &schema = *function.schema_root.arrow_schema.children[col_idx];
			arrow_types.at(col_idx)->ThrowIfInvalid();
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
	if (!input.projection_ids.empty()) {
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
	auto result = make_uniq<ArrowScanLocalState>(std::move(current_chunk), context);
	result->column_ids = input.column_ids;
	result->filters = input.filters.get();
	auto &bind_data = input.bind_data->Cast<ArrowScanFunctionData>();
	if (!bind_data.projection_pushdown_enabled) {
		result->column_ids.clear();
	} else if (!input.projection_ids.empty()) {
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
	if (state.chunk_offset >= static_cast<idx_t>(state.chunk->arrow_array.length)) {
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

OperatorPartitionData ArrowTableFunction::ArrowGetPartitionData(ClientContext &context,
                                                                TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("ArrowTableFunction::GetPartitionData: partition columns not supported");
	}
	auto &state = input.local_state->Cast<ArrowScanLocalState>();
	return OperatorPartitionData(state.batch_index);
}

bool ArrowTableFunction::ArrowPushdownType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return true;
	case LogicalTypeId::DECIMAL: {
		switch (type.InternalType()) {
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
			return true;
		default:
			return false;
		}
	}
	case LogicalTypeId::STRUCT: {
		auto struct_types = StructType::GetChildTypes(type);
		for (auto &struct_type : struct_types) {
			if (!ArrowPushdownType(struct_type.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction arrow("arrow_scan", {LogicalType::POINTER, LogicalType::POINTER, LogicalType::POINTER},
	                    ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow.cardinality = ArrowScanCardinality;
	arrow.get_partition_data = ArrowGetPartitionData;
	arrow.projection_pushdown = true;
	arrow.filter_pushdown = true;
	arrow.filter_prune = true;
	arrow.supports_pushdown_type = ArrowPushdownType;
	set.AddFunction(arrow);

	TableFunction arrow_dumb("arrow_scan_dumb", {LogicalType::POINTER, LogicalType::POINTER, LogicalType::POINTER},
	                         ArrowScanFunction, ArrowScanBindDumb, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow_dumb.cardinality = ArrowScanCardinality;
	arrow_dumb.get_partition_data = ArrowGetPartitionData;
	arrow_dumb.projection_pushdown = false;
	arrow_dumb.filter_pushdown = false;
	arrow_dumb.filter_prune = false;
	set.AddFunction(arrow_dumb);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
