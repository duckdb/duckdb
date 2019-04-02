#include "parser/tableref/subqueryref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

SubqueryRef::SubqueryRef(unique_ptr<QueryNode> subquery_)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_)) {
}

bool SubqueryRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (SubqueryRef *)other_;
	return subquery->Equals(other->subquery.get());
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	auto copy = make_unique<SubqueryRef>(subquery->Copy());
	copy->alias = alias;
	copy->column_name_alias = column_name_alias;
	return move(copy);
}

void SubqueryRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	subquery->Serialize(serializer);
	serializer.Write<int>(column_name_alias.size());
	for (auto &alias : column_name_alias) {
		serializer.WriteString(alias);
	}
}

unique_ptr<TableRef> SubqueryRef::Deserialize(Deserializer &source) {
	auto subquery = QueryNode::Deserialize(source);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_unique<SubqueryRef>(move(subquery));
	size_t column_count = source.Read<int>();
	for (size_t i = 0; i < column_count; i++) {
		result->column_name_alias.push_back(source.Read<string>());
	}
	return move(result);
}
