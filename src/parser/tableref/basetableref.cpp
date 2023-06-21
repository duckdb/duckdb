#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

string BaseTableRef::ToString() const {
	string result;
	result += catalog_name.empty() ? "" : (KeywordHelper::WriteOptionallyQuoted(catalog_name) + ".");
	result += schema_name.empty() ? "" : (KeywordHelper::WriteOptionallyQuoted(schema_name) + ".");
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	return BaseToString(result, column_name_alias);
}

bool BaseTableRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BaseTableRef>();
	return other.catalog_name == catalog_name && other.schema_name == schema_name && other.table_name == table_name &&
	       column_name_alias == other.column_name_alias;
}

void BaseTableRef::Serialize(FieldWriter &writer) const {
	writer.WriteString(schema_name);
	writer.WriteString(table_name);
	writer.WriteList<string>(column_name_alias);
	writer.WriteString(catalog_name);
}

void BaseTableRef::FormatSerialize(FormatSerializer &serializer) const {
	TableRef::FormatSerialize(serializer);
	serializer.WriteProperty("schema_name", schema_name);
	serializer.WriteProperty("table_name", table_name);
	serializer.WriteProperty("column_name_alias", column_name_alias);
	serializer.WriteProperty("catalog_name", catalog_name);
}

unique_ptr<TableRef> BaseTableRef::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = make_uniq<BaseTableRef>();

	deserializer.ReadProperty("schema_name", result->schema_name);
	deserializer.ReadProperty("table_name", result->table_name);
	deserializer.ReadProperty("column_name_alias", result->column_name_alias);
	deserializer.ReadProperty("catalog_name", result->catalog_name);

	return std::move(result);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(FieldReader &reader) {
	auto result = make_uniq<BaseTableRef>();

	result->schema_name = reader.ReadRequired<string>();
	result->table_name = reader.ReadRequired<string>();
	result->column_name_alias = reader.ReadRequiredList<string>();
	result->catalog_name = reader.ReadField<string>(INVALID_CATALOG);

	return std::move(result);
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_uniq<BaseTableRef>();

	copy->catalog_name = catalog_name;
	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);

	return std::move(copy);
}
} // namespace duckdb
