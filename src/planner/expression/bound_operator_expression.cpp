#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, std::move(return_type)) {
}

string BoundOperatorExpression::ToString() const {
	return OperatorExpression::ToString<BoundOperatorExpression, Expression>(*this);
}

bool BoundOperatorExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundOperatorExpression *)other_p;
	if (!ExpressionUtil::ListEquals(children, other->children)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundOperatorExpression::Copy() {
	auto copy = make_unique<BoundOperatorExpression>(type, return_type);
	copy->CopyProperties(*this);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	return std::move(copy);
}

void BoundOperatorExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(return_type);
	writer.WriteSerializableList(children);
}

unique_ptr<Expression> BoundOperatorExpression::Deserialize(ExpressionDeserializationState &state,
                                                            FieldReader &reader) {
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto children = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto result = make_unique<BoundOperatorExpression>(state.type, return_type);
	result->children = std::move(children);
	return std::move(result);
}

} // namespace duckdb
