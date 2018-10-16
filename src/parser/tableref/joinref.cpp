
#include "parser/tableref/joinref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> JoinRef::Copy() {
	auto copy = make_unique<JoinRef>();
	copy->left = left->Copy();
	copy->right = right->Copy();
	copy->condition = condition->Copy();
	copy->type = type;
	copy->alias = alias;
	return copy;
}

void JoinRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
	condition->Serialize(serializer);
	serializer.Write<JoinType>(type);
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &source) {
	bool failed = false;
	auto result = make_unique<JoinRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);
	result->condition = Expression::Deserialize(source);
	result->type = source.Read<JoinType>(failed);

	if (!result->left || !result->right || !result->condition || failed) {
		return nullptr;
	}

	return result;
}
