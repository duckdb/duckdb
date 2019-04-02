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
	for (auto &expr : using_hidden_columns) {
		unique_ptr<Expression> expr_copy = expr->Copy();
		copy->using_hidden_columns.insert(move(expr_copy));
	}
	return move(copy);
}

void JoinRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
	condition->Serialize(serializer);
	serializer.Write<JoinType>(type);

	serializer.Write<uint32_t>(using_hidden_columns.size());
	for (auto &child : using_hidden_columns) {
		child->Serialize(serializer);
	}
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &source) {
	auto result = make_unique<JoinRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);
	result->condition = ParsedExpression::Deserialize(source);
	result->type = source.Read<JoinType>();

	auto count = source.Read<uint32_t>();
	for (size_t i = 0; i < count; i++) {
		result->using_hidden_columns.insert(Expression::Deserialize(source));
	}
	return move(result);
}
