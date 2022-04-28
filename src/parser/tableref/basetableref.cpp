#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

string BaseTableRef::ToString() const {
	string schema = schema_name.empty() ? "" : KeywordHelper::WriteOptionallyQuoted(schema_name) + ".";
	string result = schema + KeywordHelper::WriteOptionallyQuoted(table_name);
	return BaseToString(result, column_name_alias);
}

bool BaseTableRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (BaseTableRef *)other_p;
	return other->schema_name == schema_name && other->table_name == table_name &&
	       column_name_alias == other->column_name_alias;
}

void BaseTableRef::Serialize(FieldWriter &writer) const {
	writer.WriteString(schema_name);
	writer.WriteString(table_name);
	writer.WriteList<string>(column_name_alias);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<BaseTableRef>();

	result->schema_name = reader.ReadRequired<string>();
	result->table_name = reader.ReadRequired<string>();
	result->column_name_alias = reader.ReadRequiredList<string>();

	return move(result);
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_unique<BaseTableRef>();

	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);

	return move(copy);
}
} // namespace duckdb
