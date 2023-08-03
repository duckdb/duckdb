#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void BoundDefaultExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(return_type);
}

unique_ptr<Expression> BoundDefaultExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	return make_uniq<BoundDefaultExpression>(return_type);
}

} // namespace duckdb
