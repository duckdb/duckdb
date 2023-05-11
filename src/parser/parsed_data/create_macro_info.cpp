#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateMacroInfo::CreateMacroInfo() : CreateFunctionInfo(CatalogType::MACRO_ENTRY, INVALID_SCHEMA) {
}

CreateMacroInfo::CreateMacroInfo(CatalogType type) : CreateFunctionInfo(type, INVALID_SCHEMA) {
}

unique_ptr<CreateInfo> CreateMacroInfo::Copy() const {
	auto result = make_uniq<CreateMacroInfo>();
	result->function = function->Copy();
	result->name = name;
	CopyProperties(*result);
	return std::move(result);
}

void CreateMacroInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(*function);
	writer.Finalize();
}

unique_ptr<CreateMacroInfo> CreateMacroInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CreateMacroInfo>();
	result->DeserializeBase(deserializer);

	FieldReader reader(deserializer);
	result->name = reader.ReadRequired<string>();
	result->function = reader.ReadRequiredSerializable<MacroFunction>();
	reader.Finalize();

	if (result->function->type == MacroType::TABLE_MACRO) {
		result->type = CatalogType::TABLE_MACRO_ENTRY;
	} else {
		result->type = CatalogType::MACRO_ENTRY;
	}
	return result;
}

} // namespace duckdb
