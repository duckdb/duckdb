#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateTableInfo::CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, INVALID_SCHEMA) {
}

CreateTableInfo::CreateTableInfo(string catalog_p, string schema_p, string name_p)
    : CreateInfo(CatalogType::TABLE_ENTRY, std::move(schema_p), std::move(catalog_p)), table(std::move(name_p)) {
}

CreateTableInfo::CreateTableInfo(SchemaCatalogEntry *schema, string name_p)
    : CreateTableInfo(schema->catalog->GetName(), schema->name, std::move(name_p)) {
}

void CreateTableInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(table);
	columns.Serialize(writer);
	writer.WriteSerializableList(constraints);
	writer.WriteOptional(query);
	writer.Finalize();
}

unique_ptr<CreateTableInfo> CreateTableInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<CreateTableInfo>();
	result->DeserializeBase(deserializer);

	FieldReader reader(deserializer);
	result->table = reader.ReadRequired<string>();
	result->columns = ColumnList::Deserialize(reader);
	result->constraints = reader.ReadRequiredSerializableList<Constraint>();
	result->query = reader.ReadOptional<SelectStatement>(nullptr);
	reader.Finalize();

	return result;
}

unique_ptr<CreateInfo> CreateTableInfo::Copy() const {
	auto result = make_unique<CreateTableInfo>(catalog, schema, table);
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	return std::move(result);
}

} // namespace duckdb
