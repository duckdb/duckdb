#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/physical_table_scan_enum.hpp"
#include "duckdb/main/settings.hpp"

#include <utility>

namespace duckdb {

static vector<idx_t> GetFullProjectionIds(const PhysicalTableScan &op) {
	vector<idx_t> result;
	result.reserve(op.column_ids.size());
	for (idx_t i = 0; i < op.column_ids.size(); i++) {
		result.push_back(i);
	}
	return result;
}

static vector<LogicalType> GetScanTypes(const PhysicalTableScan &op) {
	vector<LogicalType> result;
	result.reserve(op.column_ids.size());
	for (auto &column_index : op.column_ids) {
		auto column_id = column_index.GetPrimaryIndex();
		if (column_index.IsRowIdColumn()) {
			result.emplace_back(LogicalType::ROW_TYPE);
		} else if (column_index.IsVirtualColumn()) {
			auto entry = op.virtual_columns.find(column_id);
			if (entry == op.virtual_columns.end()) {
				throw InternalException("Virtual column not found");
			}
			result.push_back(entry->second.type);
		} else if (column_index.HasType()) {
			result.push_back(column_index.GetScanType());
		} else {
			result.push_back(op.returned_types[column_id]);
		}
	}
	return result;
}

static unique_ptr<Expression> ConvertGenericFilterExpression(const Expression &expr) {
	auto result = expr.Copy();
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    result, [&](BoundColumnRefExpression &column_ref, unique_ptr<Expression> &child) {
		    child = make_uniq<BoundReferenceExpression>(column_ref.alias, column_ref.return_type,
		                                                column_ref.binding.column_index.GetIndex());
	    });
	return result;
}

struct GenericFilterLocalState {
	GenericFilterLocalState() : sel_a(STANDARD_VECTOR_SIZE), sel_b(STANDARD_VECTOR_SIZE) {
	}

	SelectionVector sel_a;
	SelectionVector sel_b;
};

static idx_t ApplyGenericFilters(const vector<unique_ptr<ExpressionExecutor>> &executors,
                                 GenericFilterLocalState &state, DataChunk &chunk) {
	if (executors.empty() || chunk.size() == 0) {
		return chunk.size();
	}
	optional_ptr<SelectionVector> current_sel;
	idx_t current_count = chunk.size();
	bool use_first = true;
	for (auto &executor : executors) {
		auto &result_sel = use_first ? state.sel_a : state.sel_b;
		current_count = executor->SelectExpression(chunk, result_sel, current_sel, current_count);
		if (current_count == 0) {
			chunk.SetCardinality(0);
			return 0;
		}
		current_sel = &result_sel;
		use_first = !use_first;
	}
	if (current_sel) {
		chunk.Slice(*current_sel, current_count);
	}
	return current_count;
}

PhysicalTableScan::PhysicalTableScan(PhysicalPlan &physical_plan, vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<ColumnIndex> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info,
                                     vector<Value> parameters_p, virtual_column_map_t virtual_columns_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),

      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)), extra_info(std::move(extra_info)), parameters(std::move(parameters_p)),
      virtual_columns(std::move(virtual_columns_p)) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		physical_table_scan_execution_strategy = Settings::Get<DebugPhysicalTableScanExecutionStrategySetting>(context);

		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}
		function_projection_ids = op.projection_ids;
		auto filters = GetTableFilters(op);
		if (filters && filters->HasGenericFilters()) {
			use_intermediate_chunk = op.function.projection_pushdown && !op.projection_ids.empty() &&
			                         op.projection_ids.size() != op.column_ids.size();
			if (use_intermediate_chunk) {
				function_projection_ids = GetFullProjectionIds(op);
				scan_types = GetScanTypes(op);
			}
			for (auto &filter : filters->GetGenericFilters()) {
				generic_filters.push_back(ConvertGenericFilterExpression(*filter));
			}
		}

		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, function_projection_ids, filters,
			                             op.extra_info.sample_options, &op);

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(BufferAllocator::Get(context), input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].Reference(op.parameters[c]);
			}
			input_chunk.SetCardinality(1);
		}
	}

	idx_t max_threads = 0;
	PhysicalTableScanExecutionStrategy physical_table_scan_execution_strategy;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;
	vector<idx_t> function_projection_ids;
	vector<LogicalType> scan_types;
	vector<unique_ptr<Expression>> generic_filters;
	bool use_intermediate_chunk = false;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, gstate.function_projection_ids,
			                             gstate.GetTableFilters(op), op.extra_info.sample_options, &op);
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
		if (gstate.use_intermediate_chunk) {
			scan_chunk.Initialize(context.client, gstate.scan_types);
		}
		for (auto &filter : gstate.generic_filters) {
			generic_filter_executors.push_back(make_uniq<ExpressionExecutor>(context.client, *filter));
		}
		if (!generic_filter_executors.empty()) {
			generic_filter_state = make_uniq<GenericFilterLocalState>();
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
	DataChunk scan_chunk;
	vector<unique_ptr<ExpressionExecutor>> generic_filter_executors;
	unique_ptr<GenericFilterLocalState> generic_filter_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

static void ValidateAsyncStrategyResult(const PhysicalTableScanExecutionStrategy &strategy,
                                        const AsyncResultsExecutionMode &execution_mode_pre,
                                        const AsyncResultsExecutionMode &execution_mode_post,
                                        const AsyncResultType &result_pre, const AsyncResultType &result_post,
                                        const idx_t output_chunk_size) {
	auto execution_mode_pre_computed = AsyncResult::ConvertToAsyncResultExecutionMode(strategy);
	if (execution_mode_pre_computed != execution_mode_pre) {
		throw InternalException("ValidateAsyncStrategyResult: invalid conversion PhysicalTableScanExecutionStrategy to "
		                        "AsyncResultsExecutionMode, from '%s', to '%s'",
		                        EnumUtil::ToChars(strategy), EnumUtil::ToChars(execution_mode_pre));
	}

	if (execution_mode_pre != execution_mode_post) {
		throw InternalException("ValidateAsyncStrategyResult: results_execution_mode changed within table API's "
		                        "`function` call, before '%s', after '%s'",
		                        EnumUtil::ToChars(execution_mode_pre), EnumUtil::ToChars(execution_mode_post));
	}
	if (result_pre != AsyncResultType::IMPLICIT) {
		throw InternalException("ValidateAsyncStrategyResult: async_result is supposed to be IMPLICIT, was '%s', "
		                        "before table API's `function` call",
		                        EnumUtil::ToChars(result_pre));
	}
	switch (strategy) {
	case PhysicalTableScanExecutionStrategy::TASK_EXECUTOR_BUT_FORCE_SYNC_CHECKS:
		// This is a funny one, expected to throw on non-trivial workflows in this function
	case PhysicalTableScanExecutionStrategy::SYNCHRONOUS:
		switch (result_post) {
		case AsyncResultType::INVALID:
			throw InternalException("ValidateAsyncStrategyResult: found INVALID");
		case AsyncResultType::BLOCKED:
			throw InternalException("ValidateAsyncStrategyResult: found BLOCKED");
		case AsyncResultType::FINISHED:
			if (output_chunk_size > 0) {
				throw InternalException("ValidateAsyncStrategyResult: found FINISHED with non-empty chunk");
			}
			break;
		case AsyncResultType::HAVE_MORE_OUTPUT:
			if (output_chunk_size == 0) {
				throw InternalException("ValidateAsyncStrategyResult: found HAVE_MORE_OUTPUT with empty chunk");
			}
			break;
		case AsyncResultType::IMPLICIT:
			break;
		}
		break;
	default:
		if (result_post == AsyncResultType::BLOCKED) {
			if (output_chunk_size > 0) {
				throw InternalException("ValidateAsyncStrategyResult: found BLOCKED with non-empty chunk");
			}
		}
		break;
	}
}

SourceResultType PhysicalTableScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &g_state = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &l_state = input.local_state.Cast<TableScanLocalSourceState>();

	TableFunctionInput data(bind_data.get(), l_state.local_state.get(), g_state.global_state.get());

	if (function.function) {
		do {
			data.async_result = AsyncResultType::IMPLICIT;

			const auto initial_async_result = data.async_result.GetResultType();
			const auto execution_strategy = g_state.physical_table_scan_execution_strategy;
			const auto input_execution_mode = AsyncResult::ConvertToAsyncResultExecutionMode(execution_strategy);
			data.results_execution_mode = input_execution_mode;

			auto &function_chunk = g_state.use_intermediate_chunk ? l_state.scan_chunk : chunk;
			function_chunk.Reset();
			if (g_state.use_intermediate_chunk) {
				chunk.Reset();
			}

			// Actually call the function
			function.function(context.client, data, function_chunk);

			const auto output_async_result = data.async_result.GetResultType();
			const auto raw_chunk_size = function_chunk.size();

			// Compare and check whether state before and after function.function call is compatible, will throw in case
			// of inconsistencies
			ValidateAsyncStrategyResult(execution_strategy, input_execution_mode, data.results_execution_mode,
			                            initial_async_result, output_async_result, raw_chunk_size);

			switch (output_async_result) {
			case AsyncResultType::BLOCKED: {
				if (g_state.use_intermediate_chunk) {
					chunk.Reset();
				}
				D_ASSERT(data.async_result.HasTasks());
				annotated_lock_guard<annotated_mutex> guard(g_state.lock);
				if (g_state.CanBlock()) {
					data.async_result.ScheduleTasks(input.interrupt_state, context.pipeline->executor);
					return SourceResultType::BLOCKED;
				}
				return SourceResultType::FINISHED;
			}
			case AsyncResultType::FINISHED:
				if (g_state.use_intermediate_chunk) {
					chunk.Reset();
				}
				return SourceResultType::FINISHED;
			case AsyncResultType::IMPLICIT:
				if (raw_chunk_size == 0) {
					if (g_state.use_intermediate_chunk) {
						chunk.Reset();
					}
					return SourceResultType::FINISHED;
				}
				break;
			case AsyncResultType::HAVE_MORE_OUTPUT:
				break;
			default:
				throw InternalException(
				    "PhysicalTableScan::GetData call of function.function returned unexpected return '%'",
				    EnumUtil::ToChars(data.async_result.GetResultType()));
			}

			if (!l_state.generic_filter_executors.empty()) {
				ApplyGenericFilters(l_state.generic_filter_executors, *l_state.generic_filter_state, function_chunk);
			}
			if (g_state.use_intermediate_chunk) {
				if (function_chunk.size() > 0) {
					chunk.ReferenceColumns(function_chunk, projection_ids);
				} else {
					chunk.Reset();
				}
			}
		} while (chunk.size() == 0);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	if (g_state.in_out_final) {
		function.in_out_function_final(context, data, chunk);
	}
	switch (function.in_out_function(context, data, g_state.input_chunk, chunk)) {
	case OperatorResultType::BLOCKED: {
		annotated_lock_guard<annotated_mutex> guard(g_state.lock);
		return g_state.BlockSource(input.interrupt_state);
	}
	default:
		// FIXME: Handling for other cases (such as NEED_MORE_INPUT) breaks current functionality and extensions that
		// might be relying on current behaviour. Needs a rework that is not in scope
		break;
	}

	if (chunk.size() == 0 && function.in_out_function_final) {
		function.in_out_function_final(context, data, chunk);
		g_state.in_out_final = true;
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	ProgressData res;
	if (function.table_scan_progress) {
		double table_progress = function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
		if (table_progress < 0.0) {
			res.SetInvalid();
		} else {
			res.done = table_progress;
			res.total = 100.0;
			// Assume cardinality is always 1e3
			res.Normalize(1e3);
		}
	} else {
		// if table_scan_progress is not implemented we don't support this function yet in the progress bar
		res.SetInvalid();
	}
	return res;
}

bool PhysicalTableScan::SupportsPartitioning(const OperatorPartitionInfo &partition_info) const {
	if (!function.get_partition_data) {
		return false;
	}
	// FIXME: actually check if partition info is supported
	return true;
}

OperatorPartitionData PhysicalTableScan::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalSourceState &gstate_p, LocalSourceState &lstate,
                                                          const OperatorPartitionInfo &partition_info) const {
	D_ASSERT(SupportsPartitioning(partition_info));
	D_ASSERT(function.get_partition_data);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionGetPartitionInput input(bind_data.get(), state.local_state.get(), gstate.global_state.get(),
	                                     partition_info);
	return function.get_partition_data(context.client, input);
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + (function.extra_info.empty() ? "" : " " + function.extra_info));
}

void AddProjectionNames(const ColumnIndex &index, const string &name, const LogicalType &type, string &result) {
	if (!index.HasChildren()) {
		// base case - no children projected out
		if (!result.empty()) {
			result += "\n";
		}
		result += name;
		return;
	}

	if (type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		for (auto &child_index : index.GetChildIndexes()) {
			if (child_index.HasPrimaryIndex()) {
				auto &ele = child_types[child_index.GetPrimaryIndex()];
				AddProjectionNames(child_index, name + "." + ele.first, ele.second, result);
			} else {
				auto field_type = child_index.HasType() ? child_index.GetType() : LogicalType::VARIANT();
				AddProjectionNames(child_index, name + "." + child_index.GetFieldName(), field_type, result);
			}
		}
	} else if (type.id() == LogicalTypeId::VARIANT) {
		for (auto &child_index : index.GetChildIndexes()) {
			D_ASSERT(!child_index.HasPrimaryIndex());
			auto field_type = child_index.HasType() ? child_index.GetType() : LogicalType::VARIANT();
			AddProjectionNames(child_index, name + "." + child_index.GetFieldName(), field_type, result);
		}
	} else {
		throw InternalException("Unexpected type (%s) in AddProjectionNames", type.ToString());
	}
}

string PhysicalTableScan::GetFilterInfo(const TableFilterSet &filter_set) const {
	string filters_info;
	bool first_item = true;
	for (auto &f : filter_set) {
		auto filter_idx = f.GetIndex();
		auto &filter = f.Filter().Cast<ExpressionFilter>();
		if (filter_idx < names.size()) {
			if (!first_item) {
				filters_info += "\n";
			}
			first_item = false;

			auto &column_id = column_ids[filter_idx];
			const auto col_id = column_id.GetPrimaryIndex();
			if (IsVirtualColumn(col_id)) {
				auto entry = virtual_columns.find(col_id);
				if (entry == virtual_columns.end()) {
					throw InternalException("Virtual column not found");
				}
				filters_info += filter.ToString(entry->second.name);
			} else {
				auto column_name = column_id.GetName(names[col_id]);
				filters_info += filter.ToString(column_name);
			}
		}
	}
	for (auto &filter : filter_set.GetGenericFilters()) {
		if (!first_item) {
			filters_info += "\n";
		}
		first_item = false;
		filters_info += filter->ToString();
	}
	return filters_info;
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (function.to_string) {
		TableFunctionToStringInput input(function, bind_data.get());
		auto to_string_result = function.to_string(input);
		for (const auto &it : to_string_result) {
			result[it.first] = it.second;
		}
	} else {
		result["Function"] = StringUtil::Upper(function.name);
	}
	if (function.projection_pushdown) {
		string projections;
		idx_t projected_column_count = function.filter_prune ? projection_ids.size() : column_ids.size();
		for (idx_t i = 0; i < projected_column_count; i++) {
			auto base_index = function.filter_prune ? projection_ids[i] : i;
			auto &column_index = column_ids[base_index];
			auto column_id = column_index.GetPrimaryIndex();
			if (column_id >= names.size()) {
				continue;
			}
			AddProjectionNames(column_index, names[column_id], returned_types[column_id], projections);
		}
		result["Projections"] = projections;
	}
	if (function.filter_pushdown && table_filters) {
		result["Filters"] = GetFilterInfo(*table_filters);
	}

	if (function.filter_pushdown && dynamic_filters && dynamic_filters->HasFilters()) {
		result["Dynamic Filters"] = GetFilterInfo(*dynamic_filters->GetFinalTableFilters(*this, nullptr));
	}

	if (extra_info.sample_options) {
		result["Sample Method"] = "System: " + extra_info.sample_options->sample_size.ToString() + "%";
	}
	if (!extra_info.file_filters.empty()) {
		result["File Filters"] = extra_info.file_filters;
		if (extra_info.filtered_files.IsValid() && extra_info.total_files.IsValid()) {
			result["Scanning Files"] = StringUtil::Format("%llu/%llu", extra_info.filtered_files.GetIndex(),
			                                              extra_info.total_files.GetIndex());
		}
	}

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function != other.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

bool PhysicalTableScan::ParallelSource() const {
	if (!function.function) {
		// table in-out functions cannot be executed in parallel as part of a PhysicalTableScan
		// since they have only a single input row
		return false;
	}
	return true;
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ExtraSourceParams(GlobalSourceState &gstate_p,
                                                                         LocalSourceState &lstate) const {
	if (!function.dynamic_to_string) {
		return InsertionOrderPreservingMap<string>();
	}
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionDynamicToStringInput input(function, bind_data.get(), state.local_state.get(),
	                                        gstate.global_state.get());
	return function.dynamic_to_string(input);
}

optional_idx PhysicalTableScan::GetRowsScanned(GlobalSourceState &gstate_p, LocalSourceState &lstate) const {
	if (function.rows_scanned) {
		auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
		auto &state = lstate.Cast<TableScanLocalSourceState>();
		return function.rows_scanned(*gstate.global_state, *state.local_state);
	}
	return optional_idx();
}

} // namespace duckdb
