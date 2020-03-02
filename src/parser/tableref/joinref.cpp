#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/common/serializer.hpp"

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
	copy->hidden_columns = hidden_columns;
	return move(copy);
}

void JoinRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
	condition->Serialize(serializer);
	serializer.Write<JoinType>(type);
	assert(hidden_columns.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)hidden_columns.size());
	for (auto &hidden_column : hidden_columns) {
		serializer.WriteString(hidden_column);
	}
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &source) {
	auto result = make_unique<JoinRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);
	result->condition = ParsedExpression::Deserialize(source);
	result->type = source.Read<JoinType>();
	auto count = source.Read<uint32_t>();
	for (idx_t i = 0; i < count; i++) {
		result->hidden_columns.insert(source.Read<string>());
	}
	return move(result);
}
