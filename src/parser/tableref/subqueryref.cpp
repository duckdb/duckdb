#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

string SubqueryRef::ToString() const {
	string result = "(" + subquery->ToString() + ")";
	return BaseToString(result, column_name_alias);
}

SubqueryRef::SubqueryRef(unique_ptr<SelectStatement> subquery_p, string alias_p)
    : TableRef(TableReferenceType::SUBQUERY), subquery(std::move(subquery_p)) {
	this->alias = std::move(alias_p);
}

bool SubqueryRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (SubqueryRef *)other_p;
	return subquery->Equals(other->subquery.get());
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	auto copy = make_uniq<SubqueryRef>(unique_ptr_cast<SQLStatement, SelectStatement>(subquery->Copy()), alias);
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);
	return std::move(copy);
}

void SubqueryRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*subquery);
	writer.WriteList<string>(column_name_alias);
}

void SubqueryRef::FormatSerialize(FormatSerializer &serializer) const {
	TableRef::FormatSerialize(serializer);
	serializer.WriteProperty("subquery", subquery);
	serializer.WriteProperty("column_name_alias", column_name_alias);
}

unique_ptr<TableRef> SubqueryRef::FormatDeserialize(FormatDeserializer &deserializer) {
	auto subquery = deserializer.ReadProperty<unique_ptr<SelectStatement>>("subquery");
	auto result = make_uniq<SubqueryRef>(std::move(subquery));
	deserializer.ReadProperty("column_name_alias", result->column_name_alias);
	return std::move(result);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(FieldReader &reader) {
	auto subquery = reader.ReadRequiredSerializable<SelectStatement>();
	auto result = make_uniq<SubqueryRef>(std::move(subquery));
	result->column_name_alias = reader.ReadRequiredList<string>();
	return std::move(result);
}

} // namespace duckdb
