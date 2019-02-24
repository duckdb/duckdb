#include "parser/tableref/subqueryref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

SubqueryRef::SubqueryRef(unique_ptr<QueryNode> subquery_)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_)) {
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	assert(!binder);

	auto copy = make_unique<SubqueryRef>(subquery->Copy());
	copy->alias = alias;
	return move(copy);
}

void SubqueryRef::Serialize(Serializer &serializer) {
	assert(!binder);

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
	int column_count = source.Read<int>();
	for (size_t i = 0; i < column_count; i++) {
		result->column_name_alias.push_back(source.Read<string>());
	}
	return move(result);
}
