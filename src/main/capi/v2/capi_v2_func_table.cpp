#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/capi_v2/capi_v2_data_source_internal.hpp"
#include "duckdb/main/capi_v2/capi_v2_function_internal.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/capi/capi_function_signature.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb::capiv2 {

class CV2TableFunctionInfo;

//! The bind data of a bound call site: what the data source it scans with needs, and what is only known to the call.
//! It also carries a borrowed pointer back to the function info, which the registered function owns and which outlives
//! every call site bound against it.
class CV2TableFunctionData final : public CV2DataSourceBindData {
public:
	optional_ptr<const CV2TableFunctionInfo> info;

	// The static estimate a bind callback may set via table_function_bind_set_cardinality.
	idx_t cardinality = 0;
	bool cardinality_is_exact = false;
	bool cardinality_set = false;

	vector<Identifier> column_names;

	//! For a function with an operator: whether the operator receives its input from a child of the plan, rather than
	//! once from the constant arguments of the call
	bool streaming_input = false;

	auto Copy() const -> unique_ptr<FunctionData> override {
		auto copy = make_uniq<CV2TableFunctionData>();
		CopyTo(*copy);
		copy->info = info;
		copy->cardinality = cardinality;
		copy->cardinality_is_exact = cardinality_is_exact;
		copy->cardinality_set = cardinality_set;
		copy->column_names = column_names;
		copy->streaming_input = streaming_input;
		return std::move(copy);
	}
};

class CV2TableBindInfo {
public:
	//! The columns of the input of the function's operator
	optional_ptr<const vector<LogicalType>> in_input_types;
	optional_ptr<const vector<Identifier>> in_input_names;

	vector<LogicalType> out_column_types;
	vector<Identifier> out_column_names;
	idx_t out_cardinality = 0;
	bool out_cardinality_is_exact = false;
	bool out_cardinality_set = false;
	OrderPreservationType out_order_preservation = OrderPreservationType::INSERTION_ORDER;
};

static auto Convert(duckdb_v2_table_function_bind_info_handle info) -> CV2TableBindInfo * {
	return reinterpret_cast<CV2TableBindInfo *>(info);
}
static auto Convert(CV2TableBindInfo *info) -> duckdb_v2_table_function_bind_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_bind_info_handle>(info);
}

class CV2TableStatsInfo {
public:
	explicit CV2TableStatsInfo(BaseStatistics &result) : result_stats(CV2Stats::Writable(result)) {
	}

	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	idx_t in_column_index = 0;

	CV2Stats result_stats;
};

static auto Convert(duckdb_v2_table_function_stats_info_handle info) -> CV2TableStatsInfo * {
	return reinterpret_cast<CV2TableStatsInfo *>(info);
}
static auto Convert(CV2TableStatsInfo *info) -> duckdb_v2_table_function_stats_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_stats_info_handle>(info);
}

static_assert(static_cast<int>(OrderPreservationType::NO_ORDER) == DUCKDB_V2_ORDER_PRESERVATION_NO_ORDER,
              "OrderPreservationType::NO_ORDER mismatch");
static_assert(static_cast<int>(OrderPreservationType::INSERTION_ORDER) == DUCKDB_V2_ORDER_PRESERVATION_INSERTION_ORDER,
              "OrderPreservationType::INSERTION_ORDER mismatch");
static_assert(static_cast<int>(OrderPreservationType::FIXED_ORDER) == DUCKDB_V2_ORDER_PRESERVATION_FIXED_ORDER,
              "OrderPreservationType::FIXED_ORDER mismatch");

static auto CV2ConvertOrderPreservation(DUCKDB_V2_ORDER_PRESERVATION value) -> OrderPreservationType {
	if (static_cast<uint32_t>(value) > DUCKDB_V2_ORDER_PRESERVATION_FIXED_ORDER) {
		throw duckdb::InvalidInputException("Invalid value in duckdb_v2_table_function_bind_set_order_preservation");
	}
	return static_cast<OrderPreservationType>(value);
}

class CV2TableFunctionInfo : public TableFunctionInfo {
public:
	duckdb_v2_table_function_bind_callback_fn bind_cb = nullptr;
	duckdb_v2_table_function_stats_callback_fn stats_cb = nullptr;
	duckdb_v2_table_function_get_bind_info_callback_fn get_bind_info_cb = nullptr;
	duckdb_v2_table_function_to_string_callback_fn to_string_cb = nullptr;
	duckdb_v2_table_function_partitioning_callback_fn partitioning_cb = nullptr;
	shared_ptr<CV2UserData> user_data = nullptr;
	//! Whether the function can produce the file_row_number virtual column
	bool file_row_number = false;
	//! How the scan runs, as decided at plan time
	CV2ScanOptions scan;
	//! The data source the function scans with. The bind data of a call refers to it, which the function outlives.
	CV2DataSourceInfo source;
	//! The operator producing the rows of a table in-out function
	shared_ptr<const CV2OperatorInfo> op;
	//! Whether the operator's input is a table argument, rather than the positional arguments
	bool table_input = false;

	Identifier name;
};

//! What the "get bind info" callback describes a bound call with
class CV2TableGetBindInfoInfo {
public:
	explicit CV2TableGetBindInfoInfo(const CV2TableFunctionData &bind_data_p) : bind_data(bind_data_p) {
	}

	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	//! The bound call - its result columns are the columns identifiers are attached to
	const CV2TableFunctionData &bind_data;

	//! What the callback reports
	BindInfo out_bind_info {ScanType::EXTERNAL};
};

static auto Convert(duckdb_v2_table_function_get_bind_info_info_handle info) -> CV2TableGetBindInfoInfo * {
	return reinterpret_cast<CV2TableGetBindInfoInfo *>(info);
}
static auto Convert(CV2TableGetBindInfoInfo *info) -> duckdb_v2_table_function_get_bind_info_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_get_bind_info_info_handle>(info);
}

//! Asks the function to describe a bound call: e.g. the identifiers of its columns
static auto CV2TableGetBindInfo(TableFunctionGetBindInfoInput &input) -> BindInfo {
	if (!input.bind_data) {
		return BindInfo(ScanType::EXTERNAL);
	}
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TableGetBindInfoInfo args(bind_data);
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(input.context);
	info.get_bind_info_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(args.out_bind_info);
}

//! What the "to string" callback describes a bound call with in a query plan
class CV2TableToStringInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;

	InsertionOrderPreservingMap<string> out_entries;
};

static auto Convert(duckdb_v2_table_function_to_string_info_handle info) -> CV2TableToStringInfo * {
	return reinterpret_cast<CV2TableToStringInfo *>(info);
}
static auto Convert(CV2TableToStringInfo *info) -> duckdb_v2_table_function_to_string_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_to_string_info_handle>(info);
}

static auto CV2TableToString(TableFunctionToStringInput &input) -> InsertionOrderPreservingMap<string> {
	if (!input.bind_data) {
		return InsertionOrderPreservingMap<string>();
	}
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TableToStringInfo args;
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	info.to_string_cb(Convert(&args), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return std::move(args.out_entries);
}

static auto CV2TableBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                         vector<Identifier> &names) -> unique_ptr<FunctionData> {
	const auto &info = input.info->Cast<CV2TableFunctionInfo>();

	// the binder places every argument in its signature slot, defaults included, so the call reads them as-is
	CV2ConstantBindInfo bind_info(input.table_function, input.table_function.GetSignature(),
	                              info.user_data ? info.user_data->GetData() : nullptr, input.inputs,
	                              input.named_parameters);
	CV2TableBindInfo args = {};
	args.in_input_types = &input.input_table_types;
	args.in_input_names = &input.input_table_names;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.bind_cb(Convert(&bind_info), Convert(&args), Convert(&callback_context), &err_ptr);

	// Take ownership of whatever the callback set before reporting an error, so it is destroyed either way.
	auto result = make_uniq<CV2TableFunctionData>();
	result->info = &info;
	result->source = &info.source;
	result->scan = &info.scan;
	result->handle = std::move(bind_info.out_bind_data);
	result->cardinality = args.out_cardinality;
	result->cardinality_is_exact = args.out_cardinality_is_exact;
	result->cardinality_set = args.out_cardinality_set;
	// Without a child plan, the scan hands the operator the constant arguments as its only input
	result->streaming_input = input.input_plan && *input.input_plan;

	if (err.HasError()) {
		err.ThrowAsException();
	}

	if (args.out_column_types.empty()) {
		throw InvalidInputException("The bind callback of table function %s did not declare any result columns.",
		                            input.table_function.GetName());
	}

	// The binder hands each call site its own copy of the function, so this does not leak into other calls.
	input.table_function.order_preservation_type = args.out_order_preservation;

	result->column_types = args.out_column_types;
	result->column_names = args.out_column_names;
	return_types = std::move(args.out_column_types);
	names = std::move(args.out_column_names);
	return std::move(result);
}

//----------------------------------------------------------------------------------------------------------------------
// Operators
//----------------------------------------------------------------------------------------------------------------------
class CV2OperatorGlobalState final : public GlobalTableFunctionState {
public:
	auto MaxThreads() const -> idx_t override {
		return max_threads;
	}

	CV2UserData handle;
	idx_t max_threads = GlobalTableFunctionState::MAX_THREADS;
};

class CV2OperatorLocalState final : public LocalTableFunctionState {
public:
	CV2UserData handle;
	//! Whether the operator is done with the constant arguments the scan keeps handing it
	bool arguments_consumed = false;
	//! Whether the operator reported that it is finished along with its last rows
	bool finished = false;
	//! Written into when the engine requests no columns, as the operator produces its rows through its first column
	unique_ptr<DataChunk> row_chunk;
};

static auto CV2OperatorOf(const CV2TableFunctionData &bind_data) -> const CV2OperatorInfo & {
	D_ASSERT(bind_data.info && bind_data.info->op);
	return *bind_data.info->op;
}

static auto CV2OperatorInitGlobal(ClientContext &context, TableFunctionInitInput &input)
    -> unique_ptr<GlobalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &op = CV2OperatorOf(bind_data);

	auto result = make_uniq<CV2OperatorGlobalState>();
	if (!bind_data.streaming_input) {
		// every thread of the scan would be handed the same arguments
		result->max_threads = 1;
	}
	if (!op.init_global_cb) {
		return std::move(result);
	}

	CV2OperatorInitGlobalInfo args = {};
	args.in_user_data = op.GetUserData();
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	op.init_global_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (args.out_global_state.ptr) {
		result->handle =
		    CV2UserData(args.out_global_state.ptr, args.out_global_state.destroy, args.out_global_state.equals);
	}
	if (args.out_max_threads.IsValid() && bind_data.streaming_input) {
		result->max_threads = args.out_max_threads.GetIndex();
	}

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}
	return std::move(result);
}

static auto CV2OperatorInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                 GlobalTableFunctionState *global_state) -> unique_ptr<LocalTableFunctionState> {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &op = CV2OperatorOf(bind_data);

	auto result = make_uniq<CV2OperatorLocalState>();
	if (!op.init_local_cb) {
		return std::move(result);
	}

	CV2OperatorInitLocalInfo args = {};
	args.in_user_data = op.GetUserData();
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (global_state) {
		args.in_global_state = global_state->Cast<CV2OperatorGlobalState>().handle.GetData();
	}

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context.client);
	op.init_local_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (args.out_local_state.ptr) {
		result->handle =
		    CV2UserData(args.out_local_state.ptr, args.out_local_state.destroy, args.out_local_state.equals);
	}

	// Throw after taking ownership of the state, so that it is destroyed even if we error.
	if (err.HasError()) {
		err.ThrowAsException();
	}
	return std::move(result);
}

//! The chunk the operator writes into: the output chunk, or the local row chunk when the engine requests no columns
static auto CV2OperatorTarget(ClientContext &context, const CV2TableFunctionData &bind_data,
                              CV2OperatorLocalState &local_state, DataChunk &output) -> DataChunk & {
	if (output.ColumnCount() > 0) {
		return output;
	}
	if (!local_state.row_chunk) {
		local_state.row_chunk = make_uniq<DataChunk>();
		local_state.row_chunk->Initialize(context, {bind_data.column_types[0]});
	}
	local_state.row_chunk->Reset();
	return *local_state.row_chunk;
}

//! Takes the row count of what the operator wrote from its first vector, as the exec callback does
static auto CV2OperatorSetCardinality(const CV2TableFunctionData &bind_data, DataChunk &target, DataChunk &output)
    -> void {
	auto row_count = target.data[0].size();
	if (row_count > target.GetCapacity()) {
		throw InvalidInputException("The operator of table function %s produced %llu rows, more than the %llu rows "
		                            "an output chunk holds.",
		                            bind_data.info->name, row_count, target.GetCapacity());
	}
	if (output.ColumnCount() == 0) {
		output.SetCardinalityUnsafe(row_count);
		return;
	}
	output.SetChildCardinality(row_count);
}

static auto CV2OperatorCallExecute(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                   DataChunk &output) -> OperatorResultType {
	const auto &bind_data = data.bind_data->Cast<CV2TableFunctionData>();
	const auto &op = CV2OperatorOf(bind_data);
	auto &local_state = data.local_state->Cast<CV2OperatorLocalState>();

	CV2OperatorExecuteInfo args = {};
	args.in_user_data = op.GetUserData();
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (data.global_state) {
		args.in_global_state = data.global_state->Cast<CV2OperatorGlobalState>().handle.GetData();
	}
	args.in_local_state = local_state.handle.GetData();
	auto &target = CV2OperatorTarget(context.client, bind_data, local_state, output);
	args.input = &input;
	args.output = &target;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context.client);
	op.execute_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
	CV2OperatorSetCardinality(bind_data, target, output);
	return args.out_result;
}

//! Runs the operator for the in-out function. Called with constant arguments, the table scan hands the operator the
//! same input chunk until it produces no rows, so the operator only sees it until it is done with it.
static auto CV2OperatorExecute(ExecutionContext &context, TableFunctionInput &data, DataChunk &input, DataChunk &output)
    -> OperatorResultType {
	const auto &bind_data = data.bind_data->Cast<CV2TableFunctionData>();
	auto &local_state = data.local_state->Cast<CV2OperatorLocalState>();
	if (bind_data.streaming_input) {
		if (local_state.finished) {
			return OperatorResultType::FINISHED;
		}
		auto result = CV2OperatorCallExecute(context, data, input, output);
		if (result == OperatorResultType::FINISHED && output.size() > 0) {
			// the engine drops the output of an operator that reports it is finished
			local_state.finished = true;
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		return result;
	}
	if (input.ColumnCount() == 0) {
		// a call without arguments is a single row without columns
		input.SetCardinalityUnsafe(1);
	}
	while (!local_state.arguments_consumed) {
		output.Reset();
		auto result = CV2OperatorCallExecute(context, data, input, output);
		if (result != OperatorResultType::HAVE_MORE_OUTPUT) {
			local_state.arguments_consumed = true;
		}
		if (output.size() > 0) {
			break;
		}
		context.client.FastInterruptCheck();
	}
	return local_state.arguments_consumed ? OperatorResultType::FINISHED : OperatorResultType::HAVE_MORE_OUTPUT;
}

static auto CV2OperatorFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output)
    -> OperatorFinalizeResultType {
	const auto &bind_data = data.bind_data->Cast<CV2TableFunctionData>();
	const auto &op = CV2OperatorOf(bind_data);
	auto &local_state = data.local_state->Cast<CV2OperatorLocalState>();

	CV2OperatorFinalizeInfo args = {};
	args.in_user_data = op.GetUserData();
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	if (data.global_state) {
		args.in_global_state = data.global_state->Cast<CV2OperatorGlobalState>().handle.GetData();
	}
	args.in_local_state = local_state.handle.GetData();
	auto &target = CV2OperatorTarget(context.client, bind_data, local_state, output);
	args.output = &target;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context.client);
	op.finalize_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}
	CV2OperatorSetCardinality(bind_data, target, output);
	return args.out_have_more_output ? OperatorFinalizeResultType::HAVE_MORE_OUTPUT
	                                 : OperatorFinalizeResultType::FINISHED;
}

//! An exact estimate also pins the maximum; otherwise only the estimate is known.
static auto CV2TableMakeStatistics(idx_t cardinality, bool is_exact) -> unique_ptr<NodeStatistics> {
	if (is_exact) {
		return make_uniq<NodeStatistics>(cardinality, cardinality);
	}
	return make_uniq<NodeStatistics>(cardinality);
}

static auto CV2TableCardinality(ClientContext &context, const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
	const auto &data = bind_data->Cast<CV2TableFunctionData>();
	if (!data.cardinality_set) {
		// No estimate at all: the optimizer falls back on its own defaults.
		return nullptr;
	}
	return CV2TableMakeStatistics(data.cardinality, data.cardinality_is_exact);
}

static auto CV2TableStats(ClientContext &context, TableFunctionGetStatisticsInput &input)
    -> unique_ptr<BaseStatistics> {
	const auto &data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *data.info;

	// The callback only describes whole declared columns, not virtual columns or fields extracted from a column
	const auto &column_index = input.column_index;
	if (!column_index.HasPrimaryIndex() || column_index.HasChildren() || column_index.IsVirtualColumn() ||
	    column_index.GetPrimaryIndex() >= data.column_types.size()) {
		return nullptr;
	}

	auto result = BaseStatistics::CreateUnknown(data.column_types[column_index.GetPrimaryIndex()]);
	CV2TableStatsInfo args(result);
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = data.handle ? data.handle->GetData() : nullptr;
	args.in_column_index = column_index.GetPrimaryIndex();

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.stats_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return result.ToUnique();
}

class CV2TablePartitioningInfo {
public:
	void *in_user_data = nullptr;
	void *in_bind_data = nullptr;
	const vector<column_t> *in_partition_ids = nullptr;

	// Not calling the setter is not a caller error: it means "not partitioned", the conservative default.
	TablePartitionInfo out_partition_info = TablePartitionInfo::NOT_PARTITIONED;
};

static auto Convert(duckdb_v2_table_function_partitioning_info_handle info) -> CV2TablePartitioningInfo * {
	return reinterpret_cast<CV2TablePartitioningInfo *>(info);
}
static auto Convert(CV2TablePartitioningInfo *info) -> duckdb_v2_table_function_partitioning_info_handle {
	return reinterpret_cast<duckdb_v2_table_function_partitioning_info_handle>(info);
}

static_assert(static_cast<int>(TablePartitionInfo::NOT_PARTITIONED) == DUCKDB_V2_TABLE_PARTITION_INFO_NOT_PARTITIONED,
              "TablePartitionInfo::NOT_PARTITIONED mismatch");
static_assert(static_cast<int>(TablePartitionInfo::SINGLE_VALUE_PARTITIONS) ==
                  DUCKDB_V2_TABLE_PARTITION_INFO_SINGLE_VALUE_PARTITIONS,
              "TablePartitionInfo::SINGLE_VALUE_PARTITIONS mismatch");
static_assert(static_cast<int>(TablePartitionInfo::OVERLAPPING_PARTITIONS) ==
                  DUCKDB_V2_TABLE_PARTITION_INFO_OVERLAPPING_PARTITIONS,
              "TablePartitionInfo::OVERLAPPING_PARTITIONS mismatch");
static_assert(static_cast<int>(TablePartitionInfo::DISJOINT_PARTITIONS) ==
                  DUCKDB_V2_TABLE_PARTITION_INFO_DISJOINT_PARTITIONS,
              "TablePartitionInfo::DISJOINT_PARTITIONS mismatch");

static auto CV2ConvertPartitionInfo(DUCKDB_V2_TABLE_PARTITION_INFO value) -> TablePartitionInfo {
	if (static_cast<uint32_t>(value) > DUCKDB_V2_TABLE_PARTITION_INFO_DISJOINT_PARTITIONS) {
		throw duckdb::InvalidInputException(
		    "Invalid value in duckdb_v2_table_function_partitioning_set_partition_info");
	}
	return static_cast<TablePartitionInfo>(value);
}

//! The optimizer hands over the predicates it would otherwise evaluate above the scan; whatever the callback accepts
//! is removed from the list, and the engine keeps applying the rest itself.
static auto CV2TableFilterPushdown(ClientContext &context, LogicalGet &get,
                                   optional_ptr<const FunctionData> bind_data_p,
                                   vector<unique_ptr<Expression>> &filters) -> void {
	if (filters.empty()) {
		return;
	}
	const auto &bind_data = bind_data_p->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TableFilterPushdownInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_filters = &filters;
	for (auto &column : get.GetColumnIds()) {
		args.in_column_ids.push_back(CV2ScanColumnIndex(column.GetPrimaryIndex(), nullptr));
	}
	args.out_handled.resize(filters.size(), false);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.scan.filter_pushdown_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	vector<unique_ptr<Expression>> remaining;
	for (idx_t i = 0; i < filters.size(); i++) {
		if (!args.out_handled[i]) {
			remaining.push_back(std::move(filters[i]));
		}
	}
	filters = std::move(remaining);
}

//! Runs at plan time and must be deterministic: the optimizer may call it for a plan shape it later discards.
static auto CV2TableGetPartitionInfo(ClientContext &context, TableFunctionPartitionInput &input) -> TablePartitionInfo {
	const auto &bind_data = input.bind_data->Cast<CV2TableFunctionData>();
	const auto &info = *bind_data.info;

	CV2TablePartitioningInfo args = {};
	args.in_user_data = info.user_data ? info.user_data->GetData() : nullptr;
	args.in_bind_data = bind_data.handle ? bind_data.handle->GetData() : nullptr;
	args.in_partition_ids = &input.partition_ids;

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.partitioning_cb(Convert(&args), Convert(&callback_context), &err_ptr);

	if (err.HasError()) {
		err.ThrowAsException();
	}

	return args.out_partition_info;
}

//! Registered only for functions that declared the file_row_number virtual column, which is all there is to know
static auto CV2TableGetFileRowNumberColumn(ClientContext &, optional_ptr<FunctionData>) -> virtual_column_map_t {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	return result;
}

class CV2TableFunction {
public:
	explicit CV2TableFunction(DatabaseInstance &db) : db(db) {
	}

	//! Validates the configuration and builds the table function to register on `target`.
	TableFunction Build(DatabaseInstance &target) {
		CheckRegistrationTarget(db, target, "table function");
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (!info.bind_cb) {
			throw InvalidInputException("Bind callback must be set for the function.");
		}
		// A table function declares the columns it returns from its bind callback, not through a return type.
		if (signature.GetReturnType().id() != LogicalTypeId::INVALID) {
			throw InvalidInputException("A table function signature cannot set a return type.");
		}

		if (info.op) {
			CheckOperator();
		} else {
			info.source.Verify();
			// The engine calls get_partition_data without a null check once get_partition_info claims partitions.
			if (info.partitioning_cb && !info.source.partition_data_cb) {
				throw InvalidInputException(
				    "The data source must have a partition data callback when the partitioning callback is set.");
			}
		}
		// Virtual columns are told apart from the declared columns by their place among the columns the scan was asked
		// for
		if (info.file_row_number && !info.scan.projection_pushdown) {
			throw InvalidInputException(
			    "A table function that produces a virtual column requires projection pushdown.");
		}

		auto function_signature = BuildSignature();
		function_signature.Verify();

		// A table function holds a signature of its own, so the declared one is handed over as-is
		TableFunction function(name, {}, CV2DataSourceExec, CV2TableBind, CV2DataSourceInitGlobal,
		                       CV2DataSourceInitLocal);
		if (info.op) {
			function = TableFunction(name, {}, nullptr, CV2TableBind, CV2OperatorInitGlobal, CV2OperatorInitLocal);
			function.in_out_function = CV2OperatorExecute;
			if (info.op->finalize_cb) {
				function.in_out_function_final = CV2OperatorFinalize;
			}
		}
		function.GetSignature() = std::move(function_signature);

		// Always wired: it serves the estimate a bind callback may set, which is not known at registration. It
		// reports no estimate when the bind callback sets none.
		function.cardinality = CV2TableCardinality;
		// The extended variant: setting the plain one would turn off pushing field extraction into the scan
		if (info.stats_cb) {
			function.statistics_extended = CV2TableStats;
		}
		if (info.get_bind_info_cb) {
			function.get_bind_info = CV2TableGetBindInfo;
		}
		if (info.to_string_cb) {
			function.to_string = CV2TableToString;
		}
		if (!info.op) {
			info.source.ConfigureScan(function);
		}
		if (info.scan.filter_pushdown_cb) {
			function.pushdown_complex_filter = CV2TableFilterPushdown;
		}
		if (info.partitioning_cb) {
			function.get_partition_info = CV2TableGetPartitionInfo;
		}
		function.projection_pushdown = info.scan.projection_pushdown;
		if (info.scan.ReadsFilters()) {
			// The engine applies the filters to what the function produces, so they only need to reach the scan
			function.filter_pushdown = true;
		}
		if (info.file_row_number) {
			function.get_virtual_columns = CV2TableGetFileRowNumberColumn;
		}

		// a function registered through the C API makes no stability promise unless one was declared
		function.stability = stability;

		// Only once nothing can refuse the registration: a refused one leaves the configuration as it was.
		// The data source gets the name for its error messages, and the user data of the function unless it has some.
		info.source.name = name;
		info.source.InheritUserData(info.user_data);
		info.scan.user_data = info.user_data;
		info.name = name;
		function.function_info = make_shared_ptr<CV2TableFunctionInfo>(std::move(info));

		return function;
	}

private:
	//! The signature of the registered function: the declared one, preceded by the table input of an operator
	FunctionSignature BuildSignature() const {
		if (!info.op || !info.table_input) {
			return signature;
		}
		// the declared parameters are all named, so the table input goes first
		auto result = signature;
		CAPIFunctionSignature::AddParameter(result, FunctionParameter(Identifier("input"), LogicalType::TABLE, {},
		                                                              FunctionParameterKind::POSITIONAL_ONLY));
		return result;
	}

	//! A function with an operator transforms an input instead of scanning: the scanning callbacks do not apply
	void CheckOperator() const {
		auto reject = [](bool set, const char *what) {
			if (set) {
				throw InvalidInputException("A table function with an operator cannot have %s.", what);
			}
		};
		reject(info.source.exec_cb, "an exec callback");
		reject(info.source.init_global_cb, "an init global callback");
		reject(info.source.init_local_cb, "an init local callback");
		reject(info.source.progress_cb, "a progress callback");
		reject(info.source.metrics_cb, "a metrics callback");
		reject(info.scan.filter_pushdown_cb, "a filter pushdown callback");
		reject(info.source.partition_data_cb, "a partition data callback");
		reject(info.partitioning_cb, "a partitioning callback");
		reject(info.source.claim_batch_cb, "a claim batch callback");
		reject(info.scan.projection_pushdown, "projection pushdown");
		reject(info.file_row_number, "virtual columns");
		if (!info.op->execute_cb) {
			throw InvalidInputException("Execute callback must be set for the operator of the function.");
		}
		if (info.table_input) {
			for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
				auto &parameter = signature.GetParameter(i);
				if (parameter.AcceptsPosition() || parameter.GetKind() == FunctionParameterKind::VAR_POSITIONAL) {
					throw InvalidInputException(
					    "A table function whose operator takes a table input cannot declare positional parameters.");
				}
			}
		} else if (info.op->finalize_cb) {
			throw InvalidInputException(
			    "The operator of a table function that takes its input from its arguments cannot have a finalize "
			    "callback.");
		}
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	FunctionSignature signature;
	CV2FunctionDocs docs;
	CV2TableFunctionInfo info;
	Identifier name;
	FunctionStability stability = FunctionStability::VOLATILE;
};

static auto Convert(duckdb_v2_table_function_handle func) -> CV2TableFunction * {
	return reinterpret_cast<CV2TableFunction *>(func);
}
static auto Convert(CV2TableFunction *func) -> duckdb_v2_table_function_handle {
	return reinterpret_cast<duckdb_v2_table_function_handle>(func);
}

//----------------------------------------------------------------------------------------------------------------------
// Multi-file functions
//----------------------------------------------------------------------------------------------------------------------
//! The overloads of a table function catalog entry, copied so they outlive the transaction that read them.
static auto CopyTableFunctions(optional_ptr<CatalogEntry> entry) -> unique_ptr<TableFunctionSet> {
	if (!entry) {
		return nullptr;
	}
	return make_uniq<TableFunctionSet>(entry->Cast<TableFunctionCatalogEntry>().functions);
}

class CV2MultiFileFunction {
public:
	explicit CV2MultiFileFunction(DatabaseInstance &db) : db(db) {
	}

	//! Validates the configuration and builds the function set to register on `target`, wrapping the overloads of
	//! the single-file function registered there.
	TableFunctionSet Build(DatabaseInstance &target, unique_ptr<TableFunctionSet> single_file_overloads) {
		CheckRegistrationTarget(db, target, "multi-file function");
		if (name.empty()) {
			throw InvalidInputException("Function name cannot be empty.");
		}
		if (single_file_function.empty()) {
			throw InvalidInputException("The single-file function must be set for the multi-file function.");
		}
		TableFunctionMultiFileSettings settings;
		settings.reader_type = reader_type;
		if (!file_extension.empty()) {
			settings.glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, file_extension);
		}
		auto single_file = SelectSingleFileFunction(std::move(single_file_overloads));
		if (single_file.function == CV2DataSourceExec && single_file.function_info) {
			// the multi-file scan claims the batches of a C API function that scans one batch at a time
			if (single_file.function_info->Cast<CV2TableFunctionInfo>().source.claim_batch_cb) {
				settings.claim_batch = CV2DataSourceClaimBatch;
			}
		}
		auto function = TableFunctionMultiFileWrapper::CreateFunctionSet(std::move(single_file), name, settings);
		return function;
	}

private:
	//! The overload of the single-file function that takes the path of the file to read
	TableFunction SelectSingleFileFunction(unique_ptr<TableFunctionSet> overloads) const {
		if (!overloads) {
			throw InvalidInputException("Table function %s to read single files with does not exist.",
			                            single_file_function);
		}
		for (auto &function : overloads->functions) {
			if (!function->function) {
				// an in-out function transforms an input rather than reading a file
				continue;
			}
			auto &signature = function->GetSignature();
			if (signature.GetRequiredParameterCount() != 1 || !signature.GetParameter(0).AcceptsPosition()) {
				continue;
			}
			auto &type = signature.GetParameter(0).GetType();
			if (type == LogicalType::VARCHAR || type.id() == LogicalTypeId::ANY) {
				return *function;
			}
		}
		throw InvalidInputException(
		    "Table function %s cannot read single files: it must take the file to read as its only positional "
		    "parameter, of type ANY (or VARCHAR).",
		    single_file_function);
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	Identifier name;
	Identifier single_file_function;
	string reader_type;
	string file_extension;
};

static auto Convert(duckdb_v2_multi_file_function_handle func) -> CV2MultiFileFunction * {
	return reinterpret_cast<CV2MultiFileFunction *>(func);
}
static auto Convert(CV2MultiFileFunction *func) -> duckdb_v2_multi_file_function_handle {
	return reinterpret_cast<duckdb_v2_multi_file_function_handle>(func);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_table_function_create(duckdb_v2_factory_handle factory,
                                                duckdb_v2_table_function_handle *function,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2TableFunction>(Convert(factory)->GetDatabase());
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_name(duckdb_v2_table_function_handle function, const char *name,
                                                  idx_t name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(
	    err, [&]() { Convert(function)->name = duckdb::Identifier(ConvertIdentifierName(name, name_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_get_docs(duckdb_v2_table_function_handle function,
                                                  duckdb_v2_function_docs_handle *docs,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(docs);
	return WithErrorHandler(err, [&]() { *docs = Convert(&Convert(function)->docs); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_get_signature(duckdb_v2_table_function_handle function,
                                                       duckdb_v2_function_signature_handle *sig,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(sig);
	return WithErrorHandler(err, [&]() { *sig = Convert(&Convert(function)->signature); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_user_data(duckdb_v2_table_function_handle function, duckdb_v2_opaque *data,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->info.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_data_source(duckdb_v2_table_function_handle function,
                                                         duckdb_v2_data_source_handle data_source,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(data_source);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(function);
		auto &source = *Convert(data_source);
		CheckRegistrationTarget(source.db, self.db, "data source");
		// The function keeps a copy: changing the data source afterwards does not change the function
		self.info.source = source.info;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_bind_callback(duckdb_v2_table_function_handle function,
                                                           duckdb_v2_table_function_bind_callback_fn callback,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.bind_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_projection_pushdown(duckdb_v2_table_function_handle function, bool enable,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.scan.projection_pushdown = enable; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_add_virtual_column(duckdb_v2_table_function_handle function,
                                                            DUCKDB_V2_TABLE_VIRTUAL_COLUMN column,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		switch (column) {
		case DUCKDB_V2_TABLE_VIRTUAL_COLUMN_FILE_ROW_NUMBER:
			Convert(function)->info.file_row_number = true;
			break;
		default:
			throw duckdb::InvalidInputException("Invalid value in duckdb_v2_table_function_add_virtual_column");
		}
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_filter_pushdown_callback(duckdb_v2_table_function_handle function,
                                                      duckdb_v2_table_function_filter_pushdown_callback_fn callback,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.scan.filter_pushdown_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_partitioning_callback(duckdb_v2_table_function_handle function,
                                                   duckdb_v2_table_function_partitioning_callback_fn callback,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.partitioning_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_stats_callback(duckdb_v2_table_function_handle function,
                                                            duckdb_v2_table_function_stats_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.stats_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_add_result_column(duckdb_v2_table_function_bind_info_handle info,
                                                                const char *name, idx_t name_length,
                                                                duckdb_v2_logical_type_handle type,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() {
		auto column_type_ref = Convert(type);
		const auto &column_type = *column_type_ref;
		// A result column carries data, so a wildcard or otherwise incomplete type has no meaning here.
		if (!column_type.IsComplete()) {
			throw duckdb::InvalidInputException("Result column type must be a fully defined concrete type");
		}
		auto &bind_info = *Convert(info);
		bind_info.out_column_names.emplace_back(ConvertIdentifierName(name, name_length));
		bind_info.out_column_types.push_back(column_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_cardinality(duckdb_v2_table_function_bind_info_handle info,
                                                              idx_t cardinality, bool is_exact,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &bind_info = *Convert(info);
		bind_info.out_cardinality = cardinality;
		bind_info.out_cardinality_is_exact = is_exact;
		bind_info.out_cardinality_set = true;
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_set_order_preservation(duckdb_v2_table_function_bind_info_handle info,
                                                                     DUCKDB_V2_ORDER_PRESERVATION order,
                                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() { Convert(info)->out_order_preservation = CV2ConvertOrderPreservation(order); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_stats_get_user_data(duckdb_v2_table_function_stats_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_stats_get_bind_data(duckdb_v2_table_function_stats_info_handle info,
                                                             void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_stats_get_column_index(duckdb_v2_table_function_stats_info_handle info,
                                                                idx_t *column_index, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() { *column_index = Convert(info)->in_column_index; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_stats_get_result_stats(duckdb_v2_table_function_stats_info_handle info,
                                                                duckdb_v2_stats_handle *stats,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(stats);
	return WithErrorHandler(err, [&]() { *stats = Convert(&Convert(info)->result_stats); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_input_column_count(duckdb_v2_table_function_bind_info_handle info,
                                                                     idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_input_types->size(); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_input_column_type(duckdb_v2_table_function_bind_info_handle info,
                                                                    idx_t index, duckdb_v2_logical_type_handle *type,
                                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &types = *Convert(info)->in_input_types;
		if (index >= types.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_bind_get_input_column_type");
		}
		*type = Convert(types[index]);
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_bind_get_input_column_name(duckdb_v2_table_function_bind_info_handle info,
                                                                    idx_t index, const char **name, idx_t *name_length,
                                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(name_length);
	return WithErrorHandler(err, [&]() {
		const auto &names = *Convert(info)->in_input_names;
		if (index >= names.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_bind_get_input_column_name");
		}
		SetStringOut(name, name_length, names[index]);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_user_data(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                       void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_bind_data(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                       void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_filter_count(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_filters->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_filter(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                    idx_t index, duckdb_v2_expression_handle *filter,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(filter);
	*filter = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto &filters = *Convert(info)->in_filters;
		if (index >= filters.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_get_filter");
		}
		*filter = Convert(filters[index].get());
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_accept(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t index,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &handled = Convert(info)->out_handled;
		if (index >= handled.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_accept");
		}
		handled[index] = true;
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_column_count(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_column_ids.size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_filter_pushdown_get_column_index(duckdb_v2_table_function_filter_pushdown_info_handle info,
                                                          idx_t index, idx_t *column_index,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = Convert(info)->in_column_ids;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_filter_pushdown_get_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_partitioning_get_user_data(duckdb_v2_table_function_partitioning_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_partitioning_get_bind_data(duckdb_v2_table_function_partitioning_info_handle info, void **data,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_partitioning_get_partition_column_count(duckdb_v2_table_function_partitioning_info_handle info,
                                                                 idx_t *count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(info)->in_partition_ids->size(); });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_partitioning_get_partition_column_index(duckdb_v2_table_function_partitioning_info_handle info,
                                                                 idx_t index, idx_t *column_index,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(column_index);
	return WithErrorHandler(err, [&]() {
		const auto &columns = *Convert(info)->in_partition_ids;
		if (index >= columns.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_partitioning_get_partition_column_index");
		}
		*column_index = columns[index];
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_partitioning_set_partition_info(duckdb_v2_table_function_partitioning_info_handle info,
                                                         DUCKDB_V2_TABLE_PARTITION_INFO partition_info,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err,
	                        [&]() { Convert(info)->out_partition_info = CV2ConvertPartitionInfo(partition_info); });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_set_operator(duckdb_v2_table_function_handle function,
                                                      duckdb_v2_operator_handle op,
                                                      DUCKDB_V2_TABLE_OPERATOR_INPUT input,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(op);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(function);
		auto &source = *Convert(op);
		CheckRegistrationTarget(source.db, self.db, "operator");
		if (static_cast<uint32_t>(input) > DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) {
			throw duckdb::InvalidInputException("Invalid value in duckdb_v2_table_function_set_operator");
		}
		self.info.op = duckdb::make_shared_ptr<CV2OperatorInfo>(source.info);
		self.info.table_input = input == DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE;
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_register_table_function(duckdb_v2_connection_handle conn,
                                                             duckdb_v2_table_function_handle function,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		auto &context = *Convert(conn)->context;
		context.RunFunctionInTransaction([&]() {
			auto &self = *Convert(function);
			auto function = self.Build(*context.db);
			auto signature = function.GetSignature();
			auto info = duckdb::CreateTableFunctionInfo(std::move(function));
			self.docs.AddTo(signature, info);
			info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
			auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
			// CreateFunction, unlike CreateTableFunction, adds the overloads to an existing function.
			catalog.CreateFunction(context, info);
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_table_function(duckdb_v2_extension_handle extension,
                                                            duckdb_v2_table_function_handle function,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(function);
		auto &loader = GetExtensionLoader(extension);
		auto function = self.Build(loader.GetDatabaseInstance());
		auto signature = function.GetSignature();
		duckdb::CreateTableFunctionInfo info(std::move(function));
		self.docs.AddTo(signature, info);
		info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
		loader.RegisterFunction(std::move(info));
	});
}

DUCKDB_V2_ERROR duckdb_v2_table_function_destroy(duckdb_v2_table_function_handle *function) {
	return WithErrorHandler(nullptr, [&]() {
		if (!function) {
			return;
		}
		if (*function) {
			delete Convert(*function);
			*function = nullptr;
		}
	});
}

//----------------------------------------------------------------------------------------------------------------------
// File readers
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_get_bind_info_callback(duckdb_v2_table_function_handle function,
                                                    duckdb_v2_table_function_get_bind_info_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.get_bind_info_cb = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_get_bind_info_get_user_data(duckdb_v2_table_function_get_bind_info_info_handle info,
                                                     void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_get_bind_info_get_bind_data(duckdb_v2_table_function_get_bind_info_info_handle info,
                                                     void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_get_bind_info_set_column_identifier(duckdb_v2_table_function_get_bind_info_info_handle info,
                                                             idx_t column_index, const idx_t *child_path,
                                                             idx_t child_path_length, duckdb_v2_value_handle identifier,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(identifier);
	return WithErrorHandler(err, [&]() {
		auto &args = *Convert(info);
		auto &bind_data = args.bind_data;
		if (column_index >= bind_data.column_types.size()) {
			throw duckdb::InvalidInputException(
			    "Index out of bounds in duckdb_v2_table_function_get_bind_info_set_column_identifier: result column "
			    "%llu has not been declared",
			    column_index);
		}
		if (child_path_length > 0 && !child_path) {
			throw duckdb::InvalidInputException("duckdb_v2_table_function_get_bind_info_set_column_identifier: the "
			                                    "child path cannot be null unless it is empty");
		}
		const auto &value = *Convert(identifier);
		const auto identifier_type = value.type().id();
		if (value.IsNull() ||
		    (identifier_type != duckdb::LogicalTypeId::INTEGER && identifier_type != duckdb::LogicalTypeId::VARCHAR)) {
			throw duckdb::InvalidInputException("duckdb_v2_table_function_get_bind_info_set_column_identifier: an "
			                                    "identifier must be a non-NULL INTEGER or VARCHAR, got %s",
			                                    value.ToString());
		}
		duckdb::vector<duckdb::idx_t> path(child_path, child_path + child_path_length);
		// verify the path addresses a nested field of the column, so a mistake is reported where it is made
		auto definition = duckdb::MultiFileColumnDefinition::CreateNested(bind_data.column_names[column_index],
		                                                                  bind_data.column_types[column_index]);
		definition.ResolveChildPath(path);

		auto &identifiers = args.out_bind_info.column_identifiers;
		for (auto &entry : identifiers) {
			if (entry.column_index == column_index && entry.child_path == path) {
				entry.identifier = value;
				return;
			}
		}
		identifiers.push_back({column_index, std::move(path), value});
	});
}

DUCKDB_V2_ERROR
duckdb_v2_table_function_get_bind_info_set_option(duckdb_v2_table_function_get_bind_info_info_handle info,
                                                  const char *key, idx_t key_length, duckdb_v2_value_handle value,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		Convert(info)->out_bind_info.options[duckdb::string(Convert(key, key_length))] = *Convert(value);
	});
}

//----------------------------------------------------------------------------------------------------------------------
// To string
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR
duckdb_v2_table_function_set_to_string_callback(duckdb_v2_table_function_handle function,
                                                duckdb_v2_table_function_to_string_callback_fn callback,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->info.to_string_cb = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_to_string_get_user_data(duckdb_v2_table_function_to_string_info_handle info,
                                                                 void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_user_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_to_string_get_bind_data(duckdb_v2_table_function_to_string_info_handle info,
                                                                 void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->in_bind_data; });
}

DUCKDB_V2_ERROR duckdb_v2_table_function_to_string_set(duckdb_v2_table_function_to_string_info_handle info,
                                                       const char *key, idx_t key_length, const char *value,
                                                       idx_t value_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_STR_ARG(value, value_length);
	return WithErrorHandler(err, [&]() {
		if (key_length == 0) {
			throw duckdb::InvalidInputException("duckdb_v2_table_function_to_string_set: the key cannot be empty");
		}
		Convert(info)->out_entries[duckdb::string(Convert(key, key_length))] =
		    duckdb::string(Convert(value, value_length));
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Multi-file functions
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_create(duckdb_v2_factory_handle factory,
                                                     duckdb_v2_multi_file_function_handle *function,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2MultiFileFunction>(Convert(factory)->GetDatabase());
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_set_name(duckdb_v2_multi_file_function_handle function, const char *name,
                                                       idx_t name_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(err, [&]() { Convert(function)->name = duckdb::Identifier(Convert(name, name_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_set_single_file_function(duckdb_v2_multi_file_function_handle function,
                                                                       const char *name, idx_t name_length,
                                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	return WithErrorHandler(
	    err, [&]() { Convert(function)->single_file_function = duckdb::Identifier(Convert(name, name_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_set_reader_type(duckdb_v2_multi_file_function_handle function,
                                                              const char *reader_type, idx_t reader_type_length,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(reader_type, reader_type_length);
	return WithErrorHandler(
	    err, [&]() { Convert(function)->reader_type = duckdb::string(Convert(reader_type, reader_type_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_set_file_extension(duckdb_v2_multi_file_function_handle function,
                                                                 const char *extension, idx_t extension_length,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(extension, extension_length);
	return WithErrorHandler(err, [&]() {
		auto file_extension = duckdb::string(Convert(extension, extension_length));
		if (duckdb::StringUtil::StartsWith(file_extension, ".")) {
			throw duckdb::InvalidInputException("The file extension \"%s\" must be given without a leading dot",
			                                    file_extension);
		}
		Convert(function)->file_extension = std::move(file_extension);
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_register_multi_file_function(duckdb_v2_connection_handle conn,
                                                                  duckdb_v2_multi_file_function_handle function,
                                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(function);
		auto &context = *Convert(conn)->context;
		context.RunFunctionInTransaction([&]() {
			auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
			auto single_file = catalog.GetEntry(context, duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
			                                    duckdb::Identifier::DefaultSchema(), self.single_file_function,
			                                    duckdb::OnEntryNotFound::RETURN_NULL);
			duckdb::CreateTableFunctionInfo info(self.Build(*context.db, CopyTableFunctions(single_file)));
			info.on_conflict = duckdb::OnCreateConflict::ALTER_ON_CONFLICT;
			// CreateFunction, unlike CreateTableFunction, adds the overloads to an existing function.
			catalog.CreateFunction(context, info);
		});
	});
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_multi_file_function(duckdb_v2_extension_handle extension,
                                                                 duckdb_v2_multi_file_function_handle function,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(function);
		auto &loader = GetExtensionLoader(extension);
		auto single_file = CopyTableFunctions(loader.TryGetTableFunction(self.single_file_function));
		loader.RegisterFunction(self.Build(loader.GetDatabaseInstance(), std::move(single_file)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_multi_file_function_destroy(duckdb_v2_multi_file_function_handle *function) {
	return WithErrorHandler(nullptr, [&]() {
		if (!function) {
			return;
		}
		if (*function) {
			delete Convert(*function);
			*function = nullptr;
		}
	});
}
