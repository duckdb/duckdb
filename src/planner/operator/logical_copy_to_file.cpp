#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/function_serialization.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void LogicalCopyToFile::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty(200, "file_path", file_path);
	serializer.WriteProperty(201, "use_tmp_file", use_tmp_file);
	serializer.WriteProperty(202, "filename_pattern", filename_pattern);
	serializer.WriteProperty(203, "overwrite_or_ignore", overwrite_or_ignore);
	serializer.WriteProperty(204, "per_thread_output", per_thread_output);
	serializer.WriteProperty(205, "partition_output", partition_output);
	serializer.WriteProperty(206, "partition_columns", partition_columns);
	serializer.WriteProperty(207, "names", names);
	serializer.WriteProperty(208, "expected_types", expected_types);
	serializer.WriteProperty(209, "copy_info", copy_info);

	// Serialize function
	serializer.WriteProperty(210, "function_name", function.name);

	bool has_serialize = function.serialize;
	serializer.WriteProperty(211, "function_has_serialize", has_serialize);
	if (has_serialize) {
		D_ASSERT(function.deserialize); // if serialize is set, deserialize should be set as well
		serializer.WriteObject(212, "function_data",
		                       [&](Serializer &obj) { function.serialize(obj, *bind_data, function); });
	}
}

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(Deserializer &deserializer) {
	auto file_path = deserializer.ReadProperty<string>(200, "file_path");
	auto use_tmp_file = deserializer.ReadProperty<bool>(201, "use_tmp_file");
	auto filename_pattern = deserializer.ReadProperty<FilenamePattern>(202, "filename_pattern");
	auto overwrite_or_ignore = deserializer.ReadProperty<bool>(203, "overwrite_or_ignore");
	auto per_thread_output = deserializer.ReadProperty<bool>(204, "per_thread_output");
	auto partition_output = deserializer.ReadProperty<bool>(205, "partition_output");
	auto partition_columns = deserializer.ReadProperty<vector<idx_t>>(206, "partition_columns");
	auto names = deserializer.ReadProperty<vector<string>>(207, "names");
	auto expected_types = deserializer.ReadProperty<vector<LogicalType>>(208, "expected_types");
	auto copy_info =
	    unique_ptr_cast<ParseInfo, CopyInfo>(deserializer.ReadProperty<unique_ptr<ParseInfo>>(209, "copy_info"));

	// Deserialize function
	auto &context = deserializer.Get<ClientContext &>();
	auto name = deserializer.ReadProperty<string>(209, "function_name");

	auto &func_catalog_entry =
	    Catalog::GetEntry(context, CatalogType::COPY_FUNCTION_ENTRY, SYSTEM_CATALOG, DEFAULT_SCHEMA, name);
	if (func_catalog_entry.type != CatalogType::COPY_FUNCTION_ENTRY) {
		throw InternalException("DeserializeFunction - cant find catalog entry for function %s", name);
	}
	auto &function_entry = func_catalog_entry.Cast<CopyFunctionCatalogEntry>();
	auto function = function_entry.function;

	// Deserialize function data
	unique_ptr<FunctionData> bind_data;
	auto has_serialize = deserializer.ReadProperty<bool>(210, "function_has_serialize");
	if (has_serialize) {
		deserializer.ReadObject(211, "function_data",
		                        [&](Deserializer &obj) { bind_data = function.deserialize(obj, function); });
	} else {
		if (!function.copy_to_bind) {
			throw InternalException("Copy function \"%s\" has neither bind nor (de)serialize", function.name);
		}
		vector<LogicalType> bind_types;
		vector<string> bind_names;
		bind_data = function.copy_to_bind(context, *copy_info, bind_names, bind_types);
		if (bind_names != names) {
			throw InternalException("Copy function \"%s\" has different names in bind and deserialize", function.name);
		}
		if (bind_types != expected_types) {
			throw InternalException("Copy function \"%s\" has different types in bind and deserialize", function.name);
		}
	}

	auto result = make_uniq<LogicalCopyToFile>(function, std::move(bind_data), std::move(copy_info));
	result->file_path = file_path;
	result->use_tmp_file = use_tmp_file;
	result->filename_pattern = filename_pattern;
	result->overwrite_or_ignore = overwrite_or_ignore;
	result->per_thread_output = per_thread_output;
	result->partition_output = partition_output;
	result->partition_columns = partition_columns;
	result->names = names;
	result->expected_types = expected_types;

	return std::move(result);
}

idx_t LogicalCopyToFile::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
