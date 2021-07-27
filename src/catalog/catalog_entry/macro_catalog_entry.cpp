#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

#include "duckdb/common/serializer.hpp"

namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : StandardEntry(CatalogType::MACRO_ENTRY, schema, catalog, info->name), function(move(info->function)) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

void MacroCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	function->expression->Serialize(serializer);
	serializer.Write<uint32_t>((uint32_t)function->parameters.size());
	for (auto &param : function->parameters) {
		param->Serialize(serializer);
	}
	serializer.Write<uint32_t>((uint32_t)function->default_parameters.size());
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
}

unique_ptr<CreateMacroInfo> MacroCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateMacroInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	info->function = make_unique<MacroFunction>(ParsedExpression::Deserialize(source));
	auto param_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < param_count; i++) {
		info->function->parameters.push_back(ParsedExpression::Deserialize(source));
	}
	auto default_param_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}
	return info;
}

} // namespace duckdb
