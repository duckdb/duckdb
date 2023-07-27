#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

// Warning: some fields here were added while this code appears untested
// -> copy test in test/api/test_plan_serialization.cpp was commented out as WIP
void LogicalCopyToFile::Serialize(FieldWriter &writer) const {
	writer.WriteString(file_path);
	writer.WriteField(use_tmp_file);
	writer.WriteField(overwrite_or_ignore);
	writer.WriteField(per_thread_output);
	writer.WriteList<idx_t>(partition_columns);

	D_ASSERT(!function.name.empty());
	writer.WriteString(function.name);

	writer.WriteField(bind_data != nullptr);
	if (bind_data && !function.serialize) {
		throw InvalidInputException("Can't serialize copy function %s", function.name);
	}

	function.serialize(writer, *bind_data, function);
}

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto file_path = reader.ReadRequired<string>();
	auto use_tmp_file = reader.ReadRequired<bool>();
	auto overwrite_or_ignore = reader.ReadRequired<bool>();
	auto per_thread_output = reader.ReadRequired<bool>();
	auto partition_columns = reader.ReadRequiredList<idx_t>();

	auto copy_func_name = reader.ReadRequired<string>();

	auto has_bind_data = reader.ReadRequired<bool>();

	auto &context = state.gstate.context;
	auto &copy_func_catalog_entry =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, copy_func_name);
	auto &copy_func = copy_func_catalog_entry.function;

	unique_ptr<FunctionData> bind_data;
	if (has_bind_data) {
		if (!copy_func.deserialize) {
			throw SerializationException("Have bind info but no deserialization function for %s", copy_func.name);
		}
		bind_data = copy_func.deserialize(context, reader, copy_func);
	}

	auto result = make_uniq<LogicalCopyToFile>(copy_func, std::move(bind_data));
	result->file_path = file_path;
	result->use_tmp_file = use_tmp_file;
	result->overwrite_or_ignore = overwrite_or_ignore;
	result->per_thread_output = per_thread_output;
	result->partition_columns = std::move(partition_columns);
	return std::move(result);
}

void LogicalCopyToFile::FormatSerialize(FormatSerializer &serializer) const {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

unique_ptr<LogicalOperator> LogicalCopyToFile::FormatDeserialize(FormatDeserializer &deserializer) {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

idx_t LogicalCopyToFile::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
