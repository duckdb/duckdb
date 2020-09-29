#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer.hpp"

using namespace std;

namespace duckdb {

SubqueryRef::SubqueryRef(unique_ptr<QueryNode> subquery_p, string alias_p)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_p)) {
	this->alias = alias_p;
}

bool SubqueryRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (SubqueryRef *)other_;
	return subquery->Equals(other->subquery.get());
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	auto copy = make_unique<SubqueryRef>(subquery->Copy(), alias);
	copy->column_name_alias = column_name_alias;
	return move(copy);
}

void SubqueryRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	subquery->Serialize(serializer);
	serializer.WriteStringVector(column_name_alias);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(Deserializer &source) {
	auto subquery = QueryNode::Deserialize(source);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	source.ReadStringVector(result->column_name_alias);
	return move(result);
}

} // namespace duckdb
