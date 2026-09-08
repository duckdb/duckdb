#include "duckdb/common/multi_file/table_function_multi_file.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class TableFunctionMultiFileGlobalState : public GlobalTableFunctionState {};

class TableFunctionMultiFileLocalState : public LocalTableFunctionState {
public:
	explicit TableFunctionMultiFileLocalState(ClientContext &context) : thread_context(context) {
	}

	//! Thread context used to initialize the local state of the wrapped function
	ThreadContext thread_context;
	//! The reader the local state below belongs to
	optional_ptr<TableFunctionFileReader> reader;
	//! The local state of the wrapped function
	unique_ptr<LocalTableFunctionState> local_state;
};

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
TableFunctionFileReader::TableFunctionFileReader(TableFunction function_p, OpenFileInfo file_p,
                                                 named_parameter_map_t named_parameters_p, string reader_type_p)
    : BaseFileReader(std::move(file_p)), function(std::move(function_p)),
      named_parameters(std::move(named_parameters_p)), reader_type(std::move(reader_type_p)), file_is_assigned(false),
      exhausted(false) {
}

TableFunctionFileReader::~TableFunctionFileReader() = default;

string TableFunctionFileReader::GetReaderType() const {
	return reader_type;
}

void TableFunctionFileReader::BindFunction(ClientContext &context) {
	if (!function.bind) {
		throw InternalException("Table function %s cannot be wrapped in a multi file function - it has no bind",
		                        function.name);
	}
	vector<Value> inputs;
	inputs.emplace_back(file.path);
	auto parameters = named_parameters;
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	TableFunctionRef empty_ref;
	TableFunctionBindInput bind_input(inputs, parameters, input_table_types, input_table_names,
	                                  function.function_info.get(), nullptr, function, empty_ref);
	names.clear();
	types.clear();
	bind_data = function.bind(context, bind_input, types, names);
	columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(names, types);

	cardinality = optional_idx();
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());
		if (node_stats && node_stats->has_estimated_cardinality) {
			cardinality = node_stats->estimated_cardinality;
		}
	}
}

shared_ptr<BaseUnionData> TableFunctionFileReader::GetUnionData(idx_t file_idx) {
	auto result = make_shared_ptr<TableFunctionUnionData>(file);
	result->names = IdentifiersToStrings(names);
	result->types = types;
	result->cardinality = cardinality;
	if (file_idx == 0) {
		// keep the first reader around so we don't need to bind it again
		result->reader = shared_from_this();
	}
	return std::move(result);
}

unique_ptr<BaseStatistics> TableFunctionFileReader::GetStatistics(ClientContext &context, const Identifier &name) {
	if (!function.statistics || !bind_data) {
		return nullptr;
	}
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		if (names[col_idx] == name) {
			return function.statistics(context, bind_data.get(), col_idx);
		}
	}
	return nullptr;
}

void TableFunctionFileReader::AddVirtualColumn(column_t virtual_column_id) {
	throw NotImplementedException("Table function %s does not support reading virtual columns", function.name);
}

TableFunctionInitInput TableFunctionFileReader::GetInitInput() const {
	// the multi file reader does the projection/filter pruning itself - the wrapped function only needs to emit the
	// (local) columns it is asked for, in order
	vector<idx_t> projection_ids;
	return TableFunctionInitInput(bind_data.get(), column_indexes, projection_ids, filters.get());
}

optional_idx TableFunctionFileReader::MaxThreads(ClientContext &context) {
	InitializeFunctionState(context);
	if (!global_state) {
		// no global state - the wrapped function is scanned by a single thread
		return 1;
	}
	return global_state->MaxThreads();
}

void TableFunctionFileReader::InitializeFunctionState(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	if (global_state || !function.init_global) {
		return;
	}
	auto init_input = GetInitInput();
	global_state = function.init_global(context, init_input);
}

void TableFunctionFileReader::PrepareReader(ClientContext &context, GlobalTableFunctionState &) {
	InitializeFunctionState(context);
}

bool TableFunctionFileReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &,
                                                LocalTableFunctionState &lstate_p) {
	if (exhausted) {
		return false;
	}
	// the initial reader obtained during binding is never prepared - initialize it here instead
	InitializeFunctionState(context);
	auto &lstate = lstate_p.Cast<TableFunctionMultiFileLocalState>();
	if (!function.init_local) {
		// the wrapped function has no local state - it can only be scanned by a single thread
		bool expected = false;
		if (!file_is_assigned.compare_exchange_strong(expected, true)) {
			return false;
		}
		lstate.local_state = nullptr;
	} else if (lstate.reader.get() != this) {
		// we are moving to a new file - initialize a local state for it
		auto init_input = GetInitInput();
		ExecutionContext execution_context(context, lstate.thread_context, nullptr);
		lstate.local_state = function.init_local(execution_context, init_input, global_state.get());
	}
	lstate.reader = this;
	return true;
}

AsyncResult TableFunctionFileReader::Scan(ClientContext &context, GlobalTableFunctionState &,
                                          LocalTableFunctionState &lstate_p, DataChunk &chunk) {
	auto &lstate = lstate_p.Cast<TableFunctionMultiFileLocalState>();
	TableFunctionInput input(bind_data.get(), lstate.local_state.get(), global_state.get());
	// the wrapped function is always run synchronously - the multi file reader drives the async results itself
	input.async_result = AsyncResultType::IMPLICIT;
	input.results_execution_mode = AsyncResultsExecutionMode::SYNCHRONOUS;
	function.function(context, input, chunk);
	if (chunk.size() == 0) {
		// an empty chunk signals the end of the scan for this thread
		exhausted = true;
	}
	return AsyncResult::FromChunk(chunk);
}

double TableFunctionFileReader::GetProgressInFile(ClientContext &context) {
	if (!function.table_scan_progress) {
		return 0;
	}
	return function.table_scan_progress(context, bind_data.get(), global_state.get());
}

InsertionOrderPreservingMap<Value> TableFunctionFileReader::GetMetadata() const {
	return {};
}

//===--------------------------------------------------------------------===//
// Interface
//===--------------------------------------------------------------------===//
TableFunctionMultiFileWrapper::TableFunctionMultiFileWrapper(TableFunction function_p, FileGlobInput glob_input_p,
                                                             string reader_type_p)
    : function(std::move(function_p)), glob_input(std::move(glob_input_p)), reader_type(std::move(reader_type_p)) {
}

unique_ptr<MultiFileReaderInterface> TableFunctionMultiFileWrapper::CreateInterface(ClientContext &context) {
	throw InternalException("TableFunctionMultiFileWrapper is constructed from the function info instead");
}

unique_ptr<MultiFileReaderInterface> TableFunctionMultiFileWrapper::Copy() {
	return make_uniq<TableFunctionMultiFileWrapper>(function, glob_input, reader_type);
}

FileGlobInput TableFunctionMultiFileWrapper::GetGlobInput() {
	return glob_input;
}

unique_ptr<BaseFileReaderOptions> TableFunctionMultiFileWrapper::InitializeOptions(ClientContext &context,
                                                                                   optional_ptr<TableFunctionInfo>) {
	return make_uniq<TableFunctionFileReaderOptions>();
}

bool TableFunctionMultiFileWrapper::ParseOption(ClientContext &context, const Identifier &key, const Value &val,
                                                MultiFileOptions &, BaseFileReaderOptions &options_p) {
	if (function.named_parameters.find(key) == function.named_parameters.end()) {
		return false;
	}
	auto &options = options_p.Cast<TableFunctionFileReaderOptions>();
	options.named_parameters[key] = val;
	return true;
}

bool TableFunctionMultiFileWrapper::ParseCopyOption(ClientContext &context, const Identifier &key,
                                                    const vector<Value> &values, BaseFileReaderOptions &options_p,
                                                    vector<Identifier> &, vector<LogicalType> &) {
	auto entry = function.named_parameters.find(key);
	if (entry == function.named_parameters.end()) {
		return false;
	}
	auto &options = options_p.Cast<TableFunctionFileReaderOptions>();
	if (values.empty()) {
		// a bare option (e.g. "auto_detect") is equivalent to setting it to true
		options.named_parameters[key] = Value::BOOLEAN(true);
		return true;
	}
	if (values.size() != 1) {
		throw BinderException("COPY parameter %s expects a single argument", key);
	}
	auto &type = entry->second;
	options.named_parameters[key] = type.id() == LogicalTypeId::ANY ? values[0] : values[0].DefaultCastAs(type);
	return true;
}

unique_ptr<TableFunctionData>
TableFunctionMultiFileWrapper::InitializeBindData(MultiFileBindData &multi_file_data,
                                                  unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<TableFunctionMultiFileData>();
	result->options.named_parameters = std::move(options_p->Cast<TableFunctionFileReaderOptions>().named_parameters);
	return std::move(result);
}

optional_idx TableFunctionMultiFileWrapper::MaxThreads(ClientContext &context, const MultiFileBindData &bind_data,
                                                       const MultiFileGlobalState &global_state,
                                                       FileExpandResult expand_result) {
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// with multiple files the multi file reader parallelizes over the files
		return optional_idx();
	}
	// a single file - the parallelism is entirely determined by the wrapped function
	if (global_state.readers.empty() || global_state.readers[0]->file_state != MultiFileFileState::OPEN) {
		// the file has not been opened yet - we cannot know how many threads it wants
		return optional_idx();
	}
	return global_state.readers[0]->reader->Cast<TableFunctionFileReader>().MaxThreads(context);
}

void TableFunctionMultiFileWrapper::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                               vector<Identifier> &names, MultiFileBindData &bind_data) {
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(context, return_types, names, *bind_data.file_list,
	                                                                bind_data, data.options, bind_data.file_options);
}

unique_ptr<GlobalTableFunctionState>
TableFunctionMultiFileWrapper::InitializeGlobalState(ClientContext &, MultiFileBindData &, MultiFileGlobalState &) {
	return make_uniq<TableFunctionMultiFileGlobalState>();
}

unique_ptr<LocalTableFunctionState> TableFunctionMultiFileWrapper::InitializeLocalState(ClientContext &context,
                                                                                        GlobalTableFunctionState &) {
	return make_uniq<TableFunctionMultiFileLocalState>(context);
}

shared_ptr<BaseFileReader> TableFunctionMultiFileWrapper::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                       BaseFileReaderOptions &options_p,
                                                                       const MultiFileOptions &) {
	auto &options = options_p.Cast<TableFunctionFileReaderOptions>();
	auto result = make_shared_ptr<TableFunctionFileReader>(function, file, options.named_parameters, reader_type);
	result->BindFunction(context);
	return std::move(result);
}

shared_ptr<BaseFileReader> TableFunctionMultiFileWrapper::CreateReader(ClientContext &context,
                                                                       GlobalTableFunctionState &,
                                                                       const OpenFileInfo &file, idx_t,
                                                                       const MultiFileBindData &bind_data) {
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	auto result = make_shared_ptr<TableFunctionFileReader>(function, file, data.options.named_parameters, reader_type);
	result->BindFunction(context);
	return std::move(result);
}

shared_ptr<BaseFileReader> TableFunctionMultiFileWrapper::CreateReader(ClientContext &context,
                                                                       GlobalTableFunctionState &gstate,
                                                                       BaseUnionData &union_data,
                                                                       const MultiFileBindData &bind_data) {
	return CreateReader(context, gstate, union_data.file, 0, bind_data);
}

unique_ptr<NodeStatistics> TableFunctionMultiFileWrapper::GetCardinality(ClientContext &context,
                                                                         const MultiFileBindData &bind_data,
                                                                         idx_t file_count) {
	if (!bind_data.initial_reader) {
		return nullptr;
	}
	auto cardinality = bind_data.initial_reader->Cast<TableFunctionFileReader>().GetCardinality();
	if (!cardinality.IsValid()) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(cardinality.GetIndex() * file_count);
}

//===--------------------------------------------------------------------===//
// Function creation
//===--------------------------------------------------------------------===//
using TableFunctionMultiFileFunction = MultiFileFunction<TableFunctionMultiFileWrapper>;

static unique_ptr<FunctionData> TableFunctionMultiFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<Identifier> &names) {
	auto &info = input.info->Cast<TableFunctionMultiFileInfo>();
	return TableFunctionMultiFileFunction::MultiFileBindInterface(
	    context, input, return_types, names,
	    make_uniq<TableFunctionMultiFileWrapper>(info.function, info.glob_input, info.reader_type));
}

TableFunction TableFunctionMultiFileWrapper::CreateFunction(TableFunction single_file_function, Identifier name,
                                                            FileGlobInput glob_input, string reader_type) {
	if (single_file_function.GetArguments().size() != 1 ||
	    single_file_function.GetArguments()[0] != LogicalType::VARCHAR) {
		throw InternalException("Only table functions taking a single VARCHAR file path can be wrapped in a multi "
		                        "file function, %s does not",
		                        single_file_function.name);
	}
	if (reader_type.empty()) {
		reader_type = name.GetIdentifierName();
	}
	TableFunctionMultiFileFunction result(std::move(name));
	result.bind = TableFunctionMultiFileBind;
	// forward the named parameters and the pushdown capabilities of the wrapped function
	for (auto &named_parameter : single_file_function.named_parameters) {
		result.named_parameters[named_parameter.first] = named_parameter.second;
	}
	result.projection_pushdown = single_file_function.projection_pushdown;
	result.filter_pushdown = single_file_function.filter_pushdown;
	result.filter_prune = single_file_function.filter_prune;
	result.supports_pushdown_type = single_file_function.supports_pushdown_type;
	result.function_info = make_shared_ptr<TableFunctionMultiFileInfo>(std::move(single_file_function),
	                                                                   std::move(glob_input), std::move(reader_type));
	return std::move(result);
}

TableFunctionSet TableFunctionMultiFileWrapper::CreateFunctionSet(TableFunction single_file_function, Identifier name,
                                                                  FileGlobInput glob_input, string reader_type) {
	return MultiFileReader::CreateFunctionSet(CreateFunction(std::move(single_file_function), std::move(name),
	                                                         std::move(glob_input), std::move(reader_type)));
}

} // namespace duckdb
