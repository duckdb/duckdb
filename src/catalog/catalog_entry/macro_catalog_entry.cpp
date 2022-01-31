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
	serializer.Write<uint32_t>((uint32_t)function->parameters.size());
	for (auto &param : function->parameters) {
		param->Serialize(serializer);
	}
	serializer.Write<uint32_t>((uint32_t)function->default_parameters.size());
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	serializer.Write<bool>(function->isQuery() ? true : false);
	if (function->isQuery())
		function->query_node->Serialize(serializer);
	else
		function->expression->Serialize(serializer);
}

unique_ptr<CreateMacroInfo> MacroCatalogEntry::Deserialize(Deserializer &source) {
	bool is_query = false;
	auto info = make_unique<CreateMacroInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	auto param_count = source.Read<uint32_t>();
	info->function = make_unique<MacroFunction>();
	for (idx_t i = 0; i < param_count; i++) {
		info->function->parameters.push_back(ParsedExpression::Deserialize(source));
	}

	auto default_param_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}

	is_query = source.Read<bool>();
	if (is_query)
		info->function->query_node = unique_ptr<QueryNode>(QueryNode::Deserialize(source));
	else
		info->function->expression = unique_ptr<ParsedExpression>(ParsedExpression::Deserialize(source));
	return info;
}

} // namespace duckdb
