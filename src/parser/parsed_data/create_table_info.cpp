#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

CreateTableInfo::CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, INVALID_SCHEMA) {
}

CreateTableInfo::CreateTableInfo(string schema_p, string name_p)
    : CreateInfo(CatalogType::TABLE_ENTRY, move(schema_p)), table(move(name_p)) {
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
	auto result = make_unique<CreateTableInfo>(schema, table);
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	return move(result);
}

} // namespace duckdb
