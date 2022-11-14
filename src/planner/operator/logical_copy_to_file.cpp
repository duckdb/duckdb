#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

void LogicalCopyToFile::Serialize(FieldWriter &writer) const {
	writer.WriteString(file_path);
	writer.WriteField(use_tmp_file);
	writer.WriteField(is_file_and_exists);

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
	auto is_file_and_exists = reader.ReadRequired<bool>();

	auto copy_func_name = reader.ReadRequired<string>();

	auto has_bind_data = reader.ReadRequired<bool>();

	auto &context = state.gstate.context;
	auto &catalog = Catalog::GetCatalog(context);
	auto func_catalog = catalog.GetEntry(context, CatalogType::COPY_FUNCTION_ENTRY, DEFAULT_SCHEMA, copy_func_name);
	if (!func_catalog || func_catalog->type != CatalogType::COPY_FUNCTION_ENTRY) {
		throw InternalException("Cant find catalog entry for function %s", copy_func_name);
	}
	auto copy_func_catalog_entry = (CopyFunctionCatalogEntry *)func_catalog;
	CopyFunction copy_func = copy_func_catalog_entry->function;

	unique_ptr<FunctionData> bind_data;
	if (has_bind_data) {
		if (!copy_func.deserialize) {
			throw SerializationException("Have bind info but no deserialization function for %s", copy_func.name);
		}
		bind_data = copy_func.deserialize(context, reader, copy_func);
	}

	auto result = make_unique<LogicalCopyToFile>(copy_func, move(bind_data));
	result->file_path = file_path;
	result->use_tmp_file = use_tmp_file;
	result->is_file_and_exists = is_file_and_exists;
	return move(result);
}

idx_t LogicalCopyToFile::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
