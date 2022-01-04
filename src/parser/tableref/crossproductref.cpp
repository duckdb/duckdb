#include "duckdb/parser/tableref/crossproductref.hpp"

#include "duckdb/common/serializer.hpp"

namespace duckdb {

bool CrossProductRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (CrossProductRef *)other_p;
	return left->Equals(other->left.get()) && right->Equals(other->right.get());
}

unique_ptr<TableRef> CrossProductRef::Copy() {
	auto copy = make_unique<CrossProductRef>();
	copy->left = left->Copy();
	copy->right = right->Copy();
	copy->alias = alias;
	return copy;
}

void CrossProductRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<TableRef> CrossProductRef::Deserialize(Deserializer &source) {
	auto result = make_unique<CrossProductRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);

	if (!result->left || !result->right) {
		return nullptr;
	}

	return result;
}

} // namespace duckdb
