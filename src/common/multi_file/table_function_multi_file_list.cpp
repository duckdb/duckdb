#include "duckdb/common/multi_file/table_function_multi_file_list.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

static bool IsOptionsColumn(const LogicalType &type) {
	return type.id() == LogicalTypeId::MAP && MapType::KeyType(type).id() == LogicalTypeId::VARCHAR;
}

static vector<OpenFileInfo> GetInputFiles(const vector<Value> &inputs, const Identifier &function_name) {
	vector<OpenFileInfo> result;
	if (inputs.empty() || inputs[0].IsNull()) {
		return result;
	}
	auto &input = inputs[0];
	if (input.type().id() == LogicalTypeId::LIST) {
		for (auto &entry : ListValue::GetChildren(input)) {
			if (entry.IsNull()) {
				return vector<OpenFileInfo>();
			}
			result.push_back(OpenFileInfo::FromValue(entry, function_name));
		}
		return result;
	}
	auto type = input.type().id();
	if (type == LogicalTypeId::VARCHAR || type == LogicalTypeId::STRUCT || type == LogicalTypeId::VARIANT) {
		result.push_back(OpenFileInfo::FromValue(input, function_name));
	}
	return result;
}

TableFunctionMultiFileList::TableFunctionMultiFileList(ClientContext &context_p, const Identifier &function_name,
                                                       vector<Value> inputs, named_argument_map_t named_parameters)
    : LazyMultiFileList(&context_p.GetContext()), context(context_p) {
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context, QualifiedName(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), function_name));
	vector<LogicalType> input_types;
	for (auto &input : inputs) {
		input_types.push_back(input.type());
	}
	function = *entry.functions.GetFunctionByArguments(context, input_types);
	if (!function.bind || !function.function || function.init_local) {
		// functions with a local state need an execution context, which a file list does not have
		throw BinderException("Table function %s cannot provide a file list", function.name);
	}
	auto &signature = function.GetSignature();
	for (idx_t i = 0; i < inputs.size() && i < signature.GetPositionalParameterCount(); i++) {
		auto &parameter_type = signature.GetParameter(i).GetType();
		if (parameter_type.id() != LogicalTypeId::ANY && parameter_type.id() != LogicalTypeId::LIST &&
		    inputs[i].type() != parameter_type) {
			inputs[i] = inputs[i].DefaultCastAs(parameter_type);
		}
	}
	signature.FillNamedDefaults(context, named_parameters);
	input_files = GetInputFiles(inputs, function.name);

	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	TableFunctionRef empty_ref;
	BoundTableFunction bound_function(function);
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names,
	                                  function.function_info.get(), nullptr, bound_function, empty_ref);
	vector<Identifier> names;
	bind_data = function.bind(context, bind_input, return_types, names);

	auto file_type = return_types.empty() ? LogicalTypeId::INVALID : return_types[0].id();
	if (file_type != LogicalTypeId::VARCHAR && file_type != LogicalTypeId::STRUCT &&
	    file_type != LogicalTypeId::VARIANT) {
		throw BinderException("Table function %s cannot provide a file list - its first column must be a VARCHAR "
		                      "path or a file struct, not %s",
		                      function.name, return_types.empty() ? "nothing" : return_types[0].ToString());
	}
}

TableFunctionMultiFileList::~TableFunctionMultiFileList() {
}

vector<OpenFileInfo> TableFunctionMultiFileList::GetDisplayFileList(optional_idx max_files) const {
	if (input_files.empty()) {
		return LazyMultiFileList::GetDisplayFileList(max_files);
	}
	vector<OpenFileInfo> result;
	for (auto &file : input_files) {
		if (max_files.IsValid() && result.size() >= max_files.GetIndex()) {
			break;
		}
		result.push_back(file);
	}
	return result;
}

void TableFunctionMultiFileList::InitializeFunction() const {
	if (initialized) {
		return;
	}
	vector<column_t> column_ids;
	for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
		column_ids.push_back(col_idx);
	}
	vector<idx_t> projection_ids;
	TableFunctionInitInput init_input(bind_data.get(), column_ids, projection_ids, nullptr);
	if (function.init_global) {
		global_state = function.init_global(context, init_input);
	}
	chunk.Initialize(Allocator::Get(context), return_types);
	initialized = true;
}

void TableFunctionMultiFileList::ReadFiles(DataChunk &output) const {
	auto start = expanded_files.size();
	auto &files = output.data[0];
	if (files.GetType().id() == LogicalTypeId::VARCHAR) {
		for (auto entry : files.Values<string_t>()) {
			if (!entry.IsValid()) {
				throw InvalidInputException("Table function %s produced a NULL file", function.name);
			}
			expanded_files.emplace_back(entry.GetValue().GetString());
		}
	} else {
		for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
			expanded_files.push_back(OpenFileInfo::FromValue(files.GetValue(row_idx), function.name));
		}
	}
	if (output.ColumnCount() < 2 || !IsOptionsColumn(output.data[1].GetType())) {
		return;
	}
	// the options to open the files with
	auto &options = output.data[1];
	auto unpack_variant = MapType::ValueType(options.GetType()).id() == LogicalTypeId::VARIANT;
	auto list_entries = options.Values<list_entry_t>();
	auto &values = MapVector::GetValues(options);
	auto key_values = MapVector::GetKeys(options).Values<string_t>();
	// variants are decoded from their unified format, which is only built once
	RecursiveUnifiedVectorFormat variant_format;
	unique_ptr<UnifiedVariantVectorData> variant_data;
	if (unpack_variant) {
		Vector::RecursiveToUnifiedFormat(values, variant_format);
		variant_data = make_uniq<UnifiedVariantVectorData>(variant_format);
	}
	for (auto entry : list_entries) {
		if (!entry.IsValid()) {
			continue;
		}
		auto &file = expanded_files[start + entry.GetIndex()];
		if (!file.extended_info) {
			file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		}
		auto list_entry = entry.GetValue();
		file.extended_info->options.reserve(file.extended_info->options.size() + list_entry.length);
		for (idx_t child_idx = list_entry.offset; child_idx < list_entry.offset + list_entry.length; child_idx++) {
			Value value;
			if (variant_data) {
				if (!variant_data->RowIsValid(child_idx)) {
					continue;
				}
				value = VariantUtils::ConvertVariantToValue(*variant_data, child_idx, 0);
			} else {
				value = values.GetValue(child_idx);
			}
			if (value.IsNull()) {
				continue;
			}
			file.extended_info->options[key_values[child_idx].GetValue().GetString()] = std::move(value);
		}
	}
}

bool TableFunctionMultiFileList::ExpandNextPath() const {
	InitializeFunction();
	TableFunctionInput input(bind_data.get(), nullptr, global_state.get());
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
