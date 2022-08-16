#include "duckdb/parser/tableref/crossproductref.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string CrossProductRef::ToString() const {
	return left->ToString() + ", " + right->ToString();
}

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
	return move(copy);
}

void CrossProductRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
}

unique_ptr<TableRef> CrossProductRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<CrossProductRef>();

	result->left = reader.ReadRequiredSerializable<TableRef>();
	result->right = reader.ReadRequiredSerializable<TableRef>();
	D_ASSERT(result->left);
	D_ASSERT(result->right);

	return move(result);
}

} // namespace duckdb
