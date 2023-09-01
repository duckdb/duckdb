#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/scalar_macro_function.hpp"

namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info,
                                     optional_ptr<ClientContext> context)
    : FunctionEntry(
          (info.function->type == MacroType::SCALAR_MACRO ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY),
          catalog, schema, info, context),
      function(std::move(info.function)) {
	this->temporary = info.temporary;
	this->internal = info.internal;
}

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info,
                                                 optional_ptr<ClientContext> context)
    : MacroCatalogEntry(catalog, schema, info, context) {
}

TableMacroCatalogEntry::TableMacroCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info,
                                               optional_ptr<ClientContext> context)
    : MacroCatalogEntry(catalog, schema, info, context) {
}

unique_ptr<CreateInfo> MacroCatalogEntry::GetInfo() const {
	auto info = make_uniq<CreateMacroInfo>(type);
	info->catalog = catalog.GetName();
	info->schema = schema.name;
	info->name = name;
	info->function = function->Copy();
	info->dependencies = dependencies.GetLogical();
	return std::move(info);
}

} // namespace duckdb
