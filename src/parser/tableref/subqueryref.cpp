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
	assert(column_name_alias.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)column_name_alias.size());
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
	index_t column_count = (index_t)source.Read<uint32_t>();
	for (index_t i = 0; i < column_count; i++) {
		result->column_name_alias.push_back(source.Read<string>());
	}
	return move(result);
}
