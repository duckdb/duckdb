#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string SubqueryRef::ToString() const {
	string result = "(" + subquery->ToString() + ")";
	return BaseToString(result, column_name_alias);
}

SubqueryRef::SubqueryRef(unique_ptr<SelectStatement> subquery_p, string alias_p)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_p)) {
	this->alias = move(alias_p);
}

bool SubqueryRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (SubqueryRef *)other_p;
	return subquery->Equals(other->subquery.get());
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	auto copy = make_unique<SubqueryRef>(unique_ptr_cast<SQLStatement, SelectStatement>(subquery->Copy()), alias);
	copy->column_name_alias = column_name_alias;
	CopyProperties(*copy);
	return move(copy);
}

void SubqueryRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*subquery);
	writer.WriteList<string>(column_name_alias);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(FieldReader &reader) {
	auto subquery = reader.ReadRequiredSerializable<SelectStatement>();
	auto result = make_unique<SubqueryRef>(move(subquery));
	result->column_name_alias = reader.ReadRequiredList<string>();
	return move(result);
}

} // namespace duckdb
