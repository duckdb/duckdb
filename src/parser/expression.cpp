#include "parser/expression.hpp"

#include "common/serializer.hpp"
#include "common/types/hash.hpp"
#include "parser/expression/list.hpp"

using namespace duckdb;
using namespace std;

bool Expression::IsAggregate() {
	bool is_aggregate = false;
	EnumerateChildren([&](Expression *child) { is_aggregate |= child->IsAggregate(); });
	return is_aggregate;
}

bool Expression::IsWindow() {
	bool is_window = false;
	EnumerateChildren([&](Expression *child) { is_window |= child->IsWindow(); });
	return is_window;
}

bool Expression::IsScalar() {
	bool is_scalar = true;
	EnumerateChildren([&](Expression *child) {
		if (!child->IsScalar()) {
			is_scalar = false;
		}
	});
	return is_scalar;
}

bool Expression::HasSubquery() {
	bool has_subquery = false;
	EnumerateChildren([&](Expression *child) { has_subquery |= child->HasSubquery(); });
	return has_subquery;
}

size_t Expression::ChildCount() const {
	return 0;
}

Expression *Expression::GetChild(size_t index) const {
	throw OutOfRangeException("Expression child index out of range!");
}

void Expression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                              size_t index) {
	throw OutOfRangeException("Expression child index out of range!");
}

bool Expression::Equals(const Expression *other) const {
	if (!other) {
		return false;
	}
	if (this->type != other->type) {
		return false;
	}
	if (this->return_type != other->return_type) {
		return false;
	}
	return true;
}

uint64_t Expression::Hash() const {
	uint64_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	hash = duckdb::CombineHash(hash, duckdb::Hash<uint32_t>((uint32_t)return_type));
	EnumerateChildren([&](Expression *child) { hash = CombineHash(child->Hash(), hash); });
	return hash;
}

void Expression::Serialize(Serializer &serializer) {
	serializer.Write<ExpressionClass>(GetExpressionClass());
	serializer.Write<ExpressionType>(type);
	serializer.Write<TypeId>(return_type);
	serializer.WriteString(alias);
}

unique_ptr<Expression> Expression::Deserialize(Deserializer &source) {
	auto expression_class = source.Read<ExpressionClass>();
	auto type = source.Read<ExpressionType>();
	auto return_type = source.Read<TypeId>();
	auto alias = source.Read<string>();
	unique_ptr<Expression> result;
	switch (expression_class) {
	case ExpressionClass::AGGREGATE:
		result = AggregateExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::CASE:
		result = CaseExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::CAST:
		result = CastExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::COLUMN_REF:
		result = ColumnRefExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::COMPARISON:
		result = ComparisonExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::CONJUNCTION:
		result = ConjunctionExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::CONSTANT:
		result = ConstantExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::DEFAULT:
		result = DefaultExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::FUNCTION:
		result = FunctionExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::OPERATOR:
		result = OperatorExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::STAR:
		result = StarExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::SUBQUERY:
		result = SubqueryExpression::Deserialize(type, return_type, source);
		break;
	case ExpressionClass::WINDOW:
		result = WindowExpression::Deserialize(type, return_type, source);
		break;
	default:
		throw SerializationException("Unsupported type for expression deserialization!");
	}
	result->return_type = return_type;
	result->alias = alias;
	return result;
}
