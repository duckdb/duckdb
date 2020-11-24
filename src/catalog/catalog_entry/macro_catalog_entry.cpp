#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

#include "duckdb/common/serializer.hpp"

namespace duckdb {

void MacroCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	function->expression->Serialize(serializer);
	serializer.Write<uint32_t>((uint32_t)function->parameters.size());
	for (auto &param : function->parameters) {
		param->Serialize(serializer);
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
	return info;
}

} // namespace duckdb
