#include "duckdb/planner/expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

Expression::Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type)
    : BaseExpression(type, expression_class), return_type(move(return_type)) {
}

Expression::~Expression() {
}

bool Expression::IsAggregate() const {
	bool is_aggregate = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { is_aggregate |= child.IsAggregate(); });
	return is_aggregate;
}

bool Expression::IsWindow() const {
	bool is_window = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { is_window |= child.IsWindow(); });
	return is_window;
}

bool Expression::IsScalar() const {
	bool is_scalar = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.IsScalar()) {
			is_scalar = false;
		}
	});
	return is_scalar;
}

bool Expression::HasSideEffects() const {
	bool has_side_effects = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (child.HasSideEffects()) {
			has_side_effects = true;
		}
	});
	return has_side_effects;
}

bool Expression::PropagatesNullValues() const {
	if (type == ExpressionType::OPERATOR_IS_NULL || type == ExpressionType::OPERATOR_IS_NOT_NULL ||
	    type == ExpressionType::COMPARE_NOT_DISTINCT_FROM || type == ExpressionType::COMPARE_DISTINCT_FROM ||
	    type == ExpressionType::CONJUNCTION_OR || type == ExpressionType::CONJUNCTION_AND) {
		return false;
	}
	bool propagate_null_values = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.PropagatesNullValues()) {
			propagate_null_values = false;
		}
	});
	return propagate_null_values;
}

bool Expression::IsFoldable() const {
	bool is_foldable = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.IsFoldable()) {
			is_foldable = false;
		}
	});
	return is_foldable;
}

bool Expression::HasParameter() const {
	bool has_parameter = false;
	ExpressionIterator::EnumerateChildren(*this,
	                                      [&](const Expression &child) { has_parameter |= child.HasParameter(); });
	return has_parameter;
}

bool Expression::HasSubquery() const {
	bool has_subquery = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { has_subquery |= child.HasSubquery(); });
	return has_subquery;
}

hash_t Expression::Hash() const {
	hash_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	hash = CombineHash(hash, return_type.Hash());
	ExpressionIterator::EnumerateChildren(*this,
	                                      [&](const Expression &child) { hash = CombineHash(child.Hash(), hash); });
	return hash;
}

void Expression::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<ExpressionType>(type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<Expression> Expression::Deserialize(Deserializer &source, ClientContext &context) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<ExpressionType>();

	unique_ptr<Expression> result;
	switch (type) {
	case ExpressionType::BOUND_REF:
		result = BoundReferenceExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::BOUND_COLUMN_REF:
		result = BoundColumnRefExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::BOUND_AGGREGATE:
		result = BoundAggregateExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::VALUE_CONSTANT:
		result = BoundConstantExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::BOUND_FUNCTION:
		result = BoundFunctionExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::OPERATOR_CAST:
		result = BoundCastExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::CASE_EXPR:
		result = BoundCaseExpression::Deserialize(context, type, reader);
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_BETWEEN:
	case ExpressionType::COMPARE_NOT_BETWEEN:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result = BoundComparisonExpression::Deserialize(context, type, reader);
		break;
	default:
		throw SerializationException("Unsupported type for expression deserialization %s",
		                             ExpressionTypeToString(type));
	}
	reader.Finalize();
	return result;
}

} // namespace duckdb
