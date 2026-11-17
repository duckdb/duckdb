#include "duckdb/common/multi_file/table_function_multi_file_list.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

static void VerifyFileColumn(const TableFunction &function, const vector<LogicalType> &return_types) {
	if (!function.function) {
		throw InternalException("Table function %s cannot provide a file list - it has no regular function",
		                        function.name);
	}
	if (return_types.empty()) {
		throw InternalException("Table function %s cannot provide a file list - it produces no columns", function.name);
	}
	auto type = return_types[0].id();
	if (type != LogicalTypeId::VARCHAR && type != LogicalTypeId::STRUCT && type != LogicalTypeId::VARIANT) {
		throw BinderException("Table function %s cannot provide a file list - its first column must be a VARCHAR "
		                      "path or a file struct, not %s",
		                      function.name, return_types[0].ToString());
	}
}

TableFunctionMultiFileList::TableFunctionMultiFileList(ClientContext &context_p, TableFunction function_p,
                                                       vector<Value> inputs, named_argument_map_t named_parameters,
                                                       TableFunctionMultiFileListSettings settings_p)
    : LazyMultiFileList(&context_p.GetContext()), context(context_p), function(std::move(function_p)),
      settings(settings_p) {
	if (!function.bind) {
		throw InternalException("Table function %s cannot provide a file list - it has no bind", function.name);
	}
	function.GetSignature().FillNamedDefaults(context, named_parameters);
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	TableFunctionRef empty_ref;
	BoundTableFunction bound_function(function);
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names,
	                                  function.function_info.get(), nullptr, bound_function, empty_ref);
	vector<Identifier> names;
	bind_data = function.bind(context, bind_input, return_types, names);
	VerifyFileColumn(function, return_types);
}

TableFunctionMultiFileList::TableFunctionMultiFileList(ClientContext &context_p, TableFunction function_p,
                                                       unique_ptr<FunctionData> bind_data_p,
                                                       vector<LogicalType> return_types_p,
                                                       TableFunctionMultiFileListSettings settings_p)
    : LazyMultiFileList(&context_p.GetContext()), context(context_p), function(std::move(function_p)),
      settings(settings_p), bind_data(std::move(bind_data_p)), return_types(std::move(return_types_p)) {
	VerifyFileColumn(function, return_types);
}

TableFunctionMultiFileList::~TableFunctionMultiFileList() {
}

vector<OpenFileInfo> TableFunctionMultiFileList::GetDisplayFileList(optional_idx max_files) const {
	if (settings.display_files && bind_data) {
		return settings.display_files(*bind_data, max_files);
	}
	return LazyMultiFileList::GetDisplayFileList(max_files);
}

void TableFunctionMultiFileList::InitializeFunction() const {
	if (initialized) {
		return;
	}
	// only the files are needed - the other columns are only produced if the function cannot leave them out
	vector<column_t> column_ids;
	if (function.projection_pushdown) {
		column_ids.push_back(0);
	} else {
		for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
			column_ids.push_back(col_idx);
		}
	}
	vector<idx_t> projection_ids;
	TableFunctionInitInput init_input(bind_data.get(), column_ids, projection_ids, nullptr);
	if (function.init_global) {
		global_state = function.init_global(context, init_input);
	}
	if (function.init_local) {
		thread_context = make_uniq<ThreadContext>(context);
		ExecutionContext execution_context(context, *thread_context, nullptr);
		local_state = function.init_local(execution_context, init_input, global_state.get());
	}
	if (!settings.scan_files) {
		vector<LogicalType> chunk_types;
		for (auto &col_id : column_ids) {
			chunk_types.push_back(return_types[col_id]);
		}
		chunk.Initialize(Allocator::Get(context), chunk_types);
	}
	initialized = true;
}

void TableFunctionMultiFileList::ReadFiles(DataChunk &output) const {
	auto &files = output.data[0];
	if (files.GetType().id() == LogicalTypeId::VARCHAR) {
		for (auto entry : files.Values<string_t>()) {
			if (!entry.IsValid()) {
				throw InvalidInputException("Table function %s produced a NULL file", function.name);
			}
			expanded_files.emplace_back(entry.GetValue().GetString());
		}
		return;
	}
	// a file struct holds the options to open the file with next to its path
	for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
		expanded_files.push_back(OpenFileInfo::FromValue(files.GetValue(row_idx), function.name));
	}
}

bool TableFunctionMultiFileList::ExpandNextPath() const {
	InitializeFunction();
	TableFunctionInput input(bind_data.get(), local_state.get(), global_state.get());
	if (settings.scan_files) {
		return settings.scan_files(context, input, expanded_files);
	}
	// the function is run synchronously, outside of a pipeline
	input.async_result = AsyncResultType::IMPLICIT;
	input.results_execution_mode = AsyncResultsExecutionMode::SYNCHRONOUS;
	chunk.Reset();
	function.function(context, input, chunk);
	if (chunk.size() == 0) {
		return false;
	}
	ReadFiles(chunk);
	return true;
}

} // namespace duckdb
