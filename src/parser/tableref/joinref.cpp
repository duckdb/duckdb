#include "parser/tableref/joinref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool JoinRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (JoinRef *)other_;
	return left->Equals(other->left.get()) && right->Equals(other->right.get()) &&
	       condition->Equals(other->condition.get()) && type == other->type;
}

unique_ptr<TableRef> JoinRef::Copy() {
	auto copy = make_unique<JoinRef>();
	copy->left = left->Copy();
	copy->right = right->Copy();
	copy->condition = condition->Copy();
	copy->type = type;
	copy->alias = alias;
	return move(copy);
}

void JoinRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
	condition->Serialize(serializer);
	serializer.Write<JoinType>(type);
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &source) {
	auto result = make_unique<JoinRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);
	result->condition = ParsedExpression::Deserialize(source);
	result->type = source.Read<JoinType>();

	return move(result);
}
