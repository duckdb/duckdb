#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/scalar_macro_function.hpp"

namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : FunctionEntry(
          (info.function->type == MacroType::SCALAR_MACRO ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY),
          catalog, schema, info),
      function(std::move(info.function)) {
	this->temporary = info.temporary;
	this->internal = info.internal;
}

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : MacroCatalogEntry(catalog, schema, info) {
}

TableMacroCatalogEntry::TableMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info)
    : MacroCatalogEntry(catalog, schema, info) {
}

unique_ptr<CreateMacroInfo> MacroCatalogEntry::GetInfoForSerialization() const {
	auto info = make_uniq<CreateMacroInfo>(type);
	info->catalog = catalog.GetName();
	info->schema = schema.name;
	info->name = name;
	info->function = function->Copy();
	return info;
}
void MacroCatalogEntry::Serialize(Serializer &serializer) const {
	auto info = GetInfoForSerialization();
	info->Serialize(serializer);
}

unique_ptr<CreateMacroInfo> MacroCatalogEntry::Deserialize(Deserializer &main_source, ClientContext &context) {
	return unique_ptr_cast<CreateInfo, CreateMacroInfo>(CreateInfo::Deserialize(main_source));
}

} // namespace duckdb
