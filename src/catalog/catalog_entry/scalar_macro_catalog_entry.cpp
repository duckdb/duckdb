#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"

namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : StandardEntry(
          (info->function->type == MacroType::SCALAR_MACRO ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY),
          schema, catalog, info->name),
      function(std::move(info->function)) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : MacroCatalogEntry(catalog, schema, info) {
}

void ScalarMacroCatalogEntry::Serialize(Serializer &main_serializer) {
	D_ASSERT(!internal);
	auto &scalar_function = (ScalarMacroFunction &)*function;
	FieldWriter writer(main_serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(*scalar_function.expression);
	// writer.WriteSerializableList(function->parameters);
	writer.WriteSerializableList(function->parameters);
	writer.WriteField<uint32_t>((uint32_t)function->default_parameters.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<CreateMacroInfo> ScalarMacroCatalogEntry::Deserialize(Deserializer &main_source, ClientContext &context) {
	auto info = make_unique<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	FieldReader reader(main_source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	auto func = make_unique<ScalarMacroFunction>(std::move(expression));
	info->function = std::move(func);
	info->function->parameters = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto default_param_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}
	// dont like this
	// info->type=CatalogType::MACRO_ENTRY;
	reader.Finalize();
	return info;
}

TableMacroCatalogEntry::TableMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : MacroCatalogEntry(catalog, schema, info) {
}

void TableMacroCatalogEntry::Serialize(Serializer &main_serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(main_serializer);

	auto &table_function = (TableMacroFunction &)*function;
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(*table_function.query_node);
	writer.WriteSerializableList(function->parameters);
	writer.WriteField<uint32_t>((uint32_t)function->default_parameters.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<CreateMacroInfo> TableMacroCatalogEntry::Deserialize(Deserializer &main_source, ClientContext &context) {
	auto info = make_unique<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
	FieldReader reader(main_source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	auto query_node = reader.ReadRequiredSerializable<QueryNode>();
	auto table_function = make_unique<TableMacroFunction>(std::move(query_node));
	info->function = std::move(table_function);
	info->function->parameters = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto default_param_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}

	reader.Finalize();

	return info;
}

} // namespace duckdb
