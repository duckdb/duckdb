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
	//! The reader the local state below belongs to - kept alive so its resources can still be released after the
	//! multi file reader has moved on to the next file
	shared_ptr<BaseFileReader> reader;
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

void TableFunctionFileReader::BindFunction(ClientContext &context, const TableFunctionFileReaderOptions &options) {
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
	if (!options.expected_names.empty()) {
		// the schema of the scan is known upfront - bind this file against that schema
		bind_input.expected_names = options.expected_names;
		bind_input.expected_types = options.expected_types;
		bind_input.expected_bind_data = options.schema_bind_data.get();
	}
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
	result->bind_data = bind_data;
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
	lstate.reader = shared_from_this();
	if (function.claim_batch) {
		// the function scans the file in batches - claim one, so that every batch becomes a scan of its own that
		// the multi file reader can put back in order
		TableFunctionInput input(bind_data.get(), lstate.local_state.get(), global_state.get());
		return function.claim_batch(context, input);
	}
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
	if (chunk.size() == 0 && !function.claim_batch) {
		// an empty chunk signals the end of the scan for this thread - when the function scans in batches it only
		// signals the end of the current batch, and the next batch is claimed by TryInitializeScan
		exhausted = true;
	}
	return AsyncResult::FromChunk(chunk);
}

void TableFunctionFileReader::FinishBatch(ClientContext &context, LocalTableFunctionState &local_state) {
	if (!function.finish_batch) {
		return;
	}
	TableFunctionInput input(bind_data.get(), &local_state, global_state.get());
	function.finish_batch(context, input);
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
TableFunctionMultiFileWrapper::TableFunctionMultiFileWrapper(TableFunction function_p,
                                                             TableFunctionMultiFileSettings settings_p)
    : function(std::move(function_p)), settings(std::move(settings_p)) {
}

unique_ptr<MultiFileReaderInterface> TableFunctionMultiFileWrapper::CreateInterface(ClientContext &context) {
	throw InternalException("TableFunctionMultiFileWrapper is constructed from the function info instead");
}

unique_ptr<MultiFileReaderInterface> TableFunctionMultiFileWrapper::Copy() {
	return make_uniq<TableFunctionMultiFileWrapper>(function, settings);
}

FileGlobInput TableFunctionMultiFileWrapper::GetGlobInput() {
	return settings.glob_input;
}

void TableFunctionMultiFileWrapper::InitializeFileOptions(MultiFileOptions &file_options) {
	file_options.maximum_sample_files = settings.maximum_sample_files;
}

unique_ptr<BaseFileReaderOptions> TableFunctionMultiFileWrapper::InitializeOptions(ClientContext &context,
                                                                                   optional_ptr<TableFunctionInfo>) {
	return make_uniq<TableFunctionFileReaderOptions>();
}

bool TableFunctionMultiFileWrapper::ParseNamedParameter(const Identifier &key, const Value &val,
                                                        TableFunctionFileReaderOptions &options) const {
	if (function.named_parameters.find(key) == function.named_parameters.end()) {
		return false;
	}
	options.named_parameters[key] = val;
	return true;
}

bool TableFunctionMultiFileWrapper::ParseOption(ClientContext &context, const Identifier &key, const Value &val,
                                                MultiFileOptions &, BaseFileReaderOptions &options_p) {
	return ParseNamedParameter(key, val, options_p.Cast<TableFunctionFileReaderOptions>());
}

bool TableFunctionMultiFileWrapper::ParseCopyOption(ClientContext &context, const Identifier &key,
                                                    const vector<Value> &values, BaseFileReaderOptions &options_p,
                                                    vector<Identifier> &, vector<LogicalType> &) {
	// COPY supports exactly the named parameters of the wrapped function - the only difference is that COPY passes
	// the values as a list, and that a bare option (e.g. "auto_detect") means "true"
	auto entry = function.named_parameters.find(key);
	if (entry == function.named_parameters.end()) {
		return false;
	}
	auto &type = entry->second;
	if (values.size() > 1) {
		throw BinderException("COPY parameter %s expects a single argument", key);
	}
	Value val;
	if (values.empty()) {
		// a bare option is a shorthand for setting a flag - only boolean parameters can be given like that
		if (type.id() != LogicalTypeId::BOOLEAN) {
			throw BinderException("COPY parameter %s expects a single argument", key);
		}
		val = Value::BOOLEAN(true);
	} else if (type.id() == LogicalTypeId::ANY || values[0].type() == type) {
		val = values[0];
	} else {
		val = values[0].DefaultCastAs(type);
	}
	return ParseNamedParameter(key, val, options_p.Cast<TableFunctionFileReaderOptions>());
}

void TableFunctionMultiFileWrapper::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
                                                     const vector<Identifier> &expected_names,
                                                     const vector<LogicalType> &expected_types) {
	// COPY takes its columns from the target table - read every file using those columns
	auto &options = options_p.Cast<TableFunctionFileReaderOptions>();
	options.expected_names = expected_names;
	options.expected_types = expected_types;
}

unique_ptr<TableFunctionData>
TableFunctionMultiFileWrapper::InitializeBindData(MultiFileBindData &multi_file_data,
                                                  unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<TableFunctionMultiFileData>();
	// the options carry the expected schema when it is known upfront (COPY takes it from the target table)
	result->options = std::move(options_p->Cast<TableFunctionFileReaderOptions>());
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

void TableFunctionMultiFileWrapper::CombineSchemas(ClientContext &context,
                                                   const vector<shared_ptr<BaseUnionData>> &union_data,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	schema_combined = true;
	if (function.combine_schema) {
		vector<reference<const FunctionData>> bind_data;
		bool have_all_bind_data = true;
		for (auto &data : union_data) {
			auto &function_data = data->Cast<TableFunctionUnionData>().bind_data;
			if (!function_data) {
				have_all_bind_data = false;
				break;
			}
			bind_data.emplace_back(*function_data);
		}
		if (have_all_bind_data) {
			TableFunctionCombineSchemaInput input(bind_data);
			combined_bind_data = function.combine_schema(context, input, return_types, names);
			if (combined_bind_data) {
				// the function combined the schemas itself - every file is read using the resulting bind data
				combined_names = names;
				combined_types = return_types;
				ReleaseBindData(union_data);
				return;
			}
			return_types.clear();
			names.clear();
		}
	}
	// fall back to combining the return types of the files
	MultiFileReaderInterface::CombineSchemas(context, union_data, return_types, names);
	combined_names = names;
	combined_types = return_types;
	ReleaseBindData(union_data);
}

void TableFunctionMultiFileWrapper::FinalizeBindData(MultiFileBindData &multi_file_data) {
	if (!schema_combined) {
		return;
	}
	auto &data = multi_file_data.bind_data->Cast<TableFunctionMultiFileData>();
	data.options.expected_names = std::move(combined_names);
	data.options.expected_types = std::move(combined_types);
	data.options.schema_bind_data = std::move(combined_bind_data);
	// the readers that were opened to combine the schemas were bound before the combined schema was known - release
	// them so that every file is bound against the combined schema instead
	multi_file_data.union_readers.clear();
	if (multi_file_data.initial_reader) {
		data.cardinality = multi_file_data.initial_reader->Cast<TableFunctionFileReader>().GetCardinality();
		multi_file_data.initial_reader = nullptr;
	}
}

//! The per-file bind data is only kept around to combine the schemas - release it afterwards so we don't hold on to
//! the bind data of every file for the duration of the query
void TableFunctionMultiFileWrapper::ReleaseBindData(const vector<shared_ptr<BaseUnionData>> &union_data) {
	for (auto &data : union_data) {
		data->Cast<TableFunctionUnionData>().bind_data.reset();
	}
}

void TableFunctionMultiFileWrapper::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                               vector<Identifier> &names, MultiFileBindData &bind_data) {
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	if (data.HasExpectedSchema()) {
		// the schema is already known (COPY) - there is no need to sample any files to determine it
		bind_data.file_options.maximum_sample_files = 1;
	}
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(context, return_types, names, *bind_data.file_list,
	                                                                bind_data, data.options, bind_data.file_options);
	if (!schema_combined && !data.HasExpectedSchema() && bind_data.initial_reader) {
		// the schema was taken from a single file - read every other file the same way that file is read
		auto &reader = bind_data.initial_reader->Cast<TableFunctionFileReader>();
		data.options.expected_names = reader.names;
		data.options.expected_types = reader.types;
		data.options.schema_bind_data = reader.bind_data;
	}
}

unique_ptr<GlobalTableFunctionState>
TableFunctionMultiFileWrapper::InitializeGlobalState(ClientContext &, MultiFileBindData &, MultiFileGlobalState &) {
	return make_uniq<TableFunctionMultiFileGlobalState>();
}

unique_ptr<LocalTableFunctionState> TableFunctionMultiFileWrapper::InitializeLocalState(ClientContext &context,
                                                                                        GlobalTableFunctionState &) {
	return make_uniq<TableFunctionMultiFileLocalState>(context);
}

void TableFunctionMultiFileWrapper::FinishReading(ClientContext &context, GlobalTableFunctionState &,
                                                  LocalTableFunctionState &lstate_p) {
	auto &lstate = lstate_p.Cast<TableFunctionMultiFileLocalState>();
	if (!lstate.reader || !lstate.local_state) {
		return;
	}
	lstate.reader->Cast<TableFunctionFileReader>().FinishBatch(context, *lstate.local_state);
}

shared_ptr<BaseFileReader> TableFunctionMultiFileWrapper::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                       BaseFileReaderOptions &options_p,
                                                                       const MultiFileOptions &) {
	auto &options = options_p.Cast<TableFunctionFileReaderOptions>();
	auto result =
	    make_shared_ptr<TableFunctionFileReader>(function, file, options.named_parameters, settings.reader_type);
	result->BindFunction(context, options);
	return std::move(result);
}

shared_ptr<BaseFileReader> TableFunctionMultiFileWrapper::CreateReader(ClientContext &context,
                                                                       GlobalTableFunctionState &,
                                                                       const OpenFileInfo &file, idx_t,
                                                                       const MultiFileBindData &bind_data) {
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	auto result =
	    make_shared_ptr<TableFunctionFileReader>(function, file, data.options.named_parameters, settings.reader_type);
	result->BindFunction(context, data.options);
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
	auto &data = bind_data.bind_data->Cast<TableFunctionMultiFileData>();
	auto cardinality = data.cardinality;
	if (bind_data.initial_reader) {
		cardinality = bind_data.initial_reader->Cast<TableFunctionFileReader>().GetCardinality();
	}
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
	    context, input, return_types, names, make_uniq<TableFunctionMultiFileWrapper>(info.function, info.settings));
}

unique_ptr<FunctionData> TableFunctionMultiFileWrapper::MultiFileBindCopy(ClientContext &context,
                                                                          CopyFromFunctionBindInput &input,
                                                                          vector<Identifier> &expected_names,
                                                                          vector<LogicalType> &expected_types) {
	auto &info = input.tf.function_info->Cast<TableFunctionMultiFileInfo>();
	return TableFunctionMultiFileFunction::MultiFileBindCopyInterface(
	    context, input, expected_names, expected_types,
	    make_uniq<TableFunctionMultiFileWrapper>(info.function, info.settings));
}

TableFunction TableFunctionMultiFileWrapper::CreateFunction(TableFunction single_file_function, Identifier name,
                                                            TableFunctionMultiFileSettings settings) {
	if (single_file_function.GetArguments().size() != 1 ||
	    single_file_function.GetArguments()[0] != LogicalType::VARCHAR) {
		throw InternalException("Only table functions taking a single VARCHAR file path can be wrapped in a multi "
		                        "file function, %s does not",
		                        single_file_function.name);
	}
	if (settings.reader_type.empty()) {
		settings.reader_type = name.GetIdentifierName();
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
	result.function_info =
	    make_shared_ptr<TableFunctionMultiFileInfo>(std::move(single_file_function), std::move(settings));
	return std::move(result);
}

TableFunctionSet TableFunctionMultiFileWrapper::CreateFunctionSet(TableFunction single_file_function, Identifier name,
                                                                  TableFunctionMultiFileSettings settings) {
	return MultiFileReader::CreateFunctionSet(
	    CreateFunction(std::move(single_file_function), std::move(name), std::move(settings)));
}

} // namespace duckdb
