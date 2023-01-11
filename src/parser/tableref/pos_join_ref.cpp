#include "duckdb/parser/tableref/pos_join_ref.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string PositionalJoinRef::ToString() const {
	return left->ToString() + " POSITIONAL JOIN " + right->ToString();
}

bool PositionalJoinRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (PositionalJoinRef *)other_p;
	return left->Equals(other->left.get()) && right->Equals(other->right.get());
}

unique_ptr<TableRef> PositionalJoinRef::Copy() {
	auto copy = make_unique<PositionalJoinRef>();
	copy->left = left->Copy();
	copy->right = right->Copy();
	copy->alias = alias;
	return std::move(copy);
}

void PositionalJoinRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
}

unique_ptr<TableRef> PositionalJoinRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<PositionalJoinRef>();

	result->left = reader.ReadRequiredSerializable<TableRef>();
	result->right = reader.ReadRequiredSerializable<TableRef>();
	D_ASSERT(result->left);
	D_ASSERT(result->right);

	return std::move(result);
}

} // namespace duckdb
