#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

// if function->expression initialized then macro is  SCALAR
//  and is stored in the SCALAR_FUNCTION catalog
//  if function->query_node iniatialized then macro is  a Table Macro
//  and is stored in the TABLE_FUNCTION catalog
MacroCatalogEntry::MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : StandardEntry(  (info->function->expression ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY),
                    schema, catalog, info->name), function(move(info->function)) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

void MacroCatalogEntry::Serialize(Serializer &main_serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(main_serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);

	if (function->expression != nullptr) {
		writer.WriteSerializable(*function->expression);
	} else {
		// cannot serialize a nullptr so use a dummy value
		auto pe= unique_ptr<ParsedExpression>();
		pe=make_unique<ConstantExpression>(Value::BIGINT(10));
		writer.WriteSerializable(*pe);
	}
	writer.WriteSerializableList(function->parameters);
	writer.WriteField<uint32_t>((uint32_t)function->default_parameters.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	if (function->query_node) {
		function->query_node->Serialize(writer);
	}
	writer.Finalize();

}

unique_ptr<CreateMacroInfo> MacroCatalogEntry::Deserialize(Deserializer &main_source) {
	auto info = make_unique<CreateMacroInfo>();

	FieldReader reader(main_source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	info->function = make_unique<MacroFunction>(move(expression));
	info->function->parameters = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto default_param_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}

	info->function->query_node=reader.ReadOptional<QueryNode>(nullptr);
	if (info->function->query_node) {
		info->function->expression.release();
	}

	// either expression or query_node but not both
	D_ASSERT ( ( info->function->expression && !info->function->query_node )  ||
	             (!info->function->expression && info->function->query_node ));


	reader.Finalize();

	return info;
}

} // namespace duckdb



