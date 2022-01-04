#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

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
	return copy;
}

void SubqueryRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	subquery->Serialize(serializer);
	serializer.WriteStringVector(column_name_alias);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(Deserializer &source) {
	auto subquery = SelectStatement::Deserialize(source);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	source.ReadStringVector(result->column_name_alias);
	return result;
}

} // namespace duckdb
