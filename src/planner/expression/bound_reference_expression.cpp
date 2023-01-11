#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

BoundReferenceExpression::BoundReferenceExpression(string alias, LogicalType type, idx_t index)
    : Expression(ExpressionType::BOUND_REF, ExpressionClass::BOUND_REF, std::move(type)), index(index) {
	this->alias = std::move(alias);
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, idx_t index)
    : BoundReferenceExpression(string(), std::move(type), index) {
}

string BoundReferenceExpression::ToString() const {
	if (!alias.empty()) {
		return alias;
	}
	return "#" + to_string(index);
}

bool BoundReferenceExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundReferenceExpression *)other_p;
	return other->index == index;
}

hash_t BoundReferenceExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<idx_t>(index));
}

unique_ptr<Expression> BoundReferenceExpression::Copy() {
	return make_unique<BoundReferenceExpression>(alias, return_type, index);
}

void BoundReferenceExpression::Serialize(FieldWriter &writer) const {
	writer.WriteString(alias);
	writer.WriteSerializable(return_type);
	writer.WriteField(index);
}

unique_ptr<Expression> BoundReferenceExpression::Deserialize(ExpressionDeserializationState &state,
                                                             FieldReader &reader) {
	auto alias = reader.ReadRequired<string>();
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto index = reader.ReadRequired<idx_t>();
	return make_unique<BoundReferenceExpression>(alias, return_type, index);
}

} // namespace duckdb
