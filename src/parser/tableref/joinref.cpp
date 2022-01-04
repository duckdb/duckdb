#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

bool JoinRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (JoinRef *)other_p;
	if (using_columns.size() != other->using_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_columns.size(); i++) {
		if (using_columns[i] != other->using_columns[i]) {
			return false;
		}
	}
	return left->Equals(other->left.get()) && right->Equals(other->right.get()) &&
	       BaseExpression::Equals(condition.get(), other->condition.get()) && type == other->type;
}

unique_ptr<TableRef> JoinRef::Copy() {
	auto copy = make_unique<JoinRef>();
	copy->left = left->Copy();
	copy->right = right->Copy();
	if (condition) {
		copy->condition = condition->Copy();
	}
	copy->type = type;
	copy->is_natural = is_natural;
	copy->alias = alias;
	copy->using_columns = using_columns;
	return copy;
}

void JoinRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	left->Serialize(serializer);
	right->Serialize(serializer);
	serializer.WriteOptional(condition);
	serializer.Write<JoinType>(type);
	serializer.Write<bool>(is_natural);
	D_ASSERT(using_columns.size() <= NumericLimits<uint32_t>::Maximum());
	serializer.Write<uint32_t>((uint32_t)using_columns.size());
	for (auto &using_column : using_columns) {
		serializer.WriteString(using_column);
	}
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &source) {
	auto result = make_unique<JoinRef>();

	result->left = TableRef::Deserialize(source);
	result->right = TableRef::Deserialize(source);
	result->condition = source.ReadOptional<ParsedExpression>();
	result->type = source.Read<JoinType>();
	result->is_natural = source.Read<bool>();
	auto count = source.Read<uint32_t>();
	for (idx_t i = 0; i < count; i++) {
		result->using_columns.push_back(source.Read<string>());
	}
	return result;
}

} // namespace duckdb
