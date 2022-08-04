#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/common/field_writer.hpp"

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

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                           FieldReader &reader) {
	// auto file_path = reader.ReadRequired<string>();
	// auto use_tmp_file = reader.ReadRequired<bool>();
	// auto is_file_and_exists = reader.ReadRequired<bool>();

	auto copy_func_name = reader.ReadRequired<string>();

	// auto has_bind_data = reader.ReadRequired<bool>();

	auto &catalog = Catalog::GetCatalog(context);
	auto func_catalog =
	    catalog.GetEntry(context, CatalogType::COPY_FUNCTION_ENTRY, DEFAULT_SCHEMA, copy_func_name, true);
	if (!func_catalog || func_catalog->type != CatalogType::COPY_FUNCTION_ENTRY) {
		throw InternalException("Cant find catalog entry for function %s", copy_func_name);
	}
	auto copy_func_catalog_entry = (CopyFunctionCatalogEntry *)func_catalog;
	// TODO(stephwang): find out how to get CopyFunction from CopyFunctionCatalogEntry
	CopyFunction func = copy_func_catalog_entry->function;
	return nullptr;
}

} // namespace duckdb
