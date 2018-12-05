#include "parser/expression/comparison_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ComparisonExpression::Copy() {
	assert(children.size() == 2);
	auto copy = make_unique<ComparisonExpression>(type, children[0]->Copy(), children[1]->Copy());
	copy->CopyProperties(*this);
	return copy;
}

unique_ptr<Expression> ComparisonExpression::Deserialize(ExpressionDeserializeInfo *info, Deserializer &source) {
	if (info->children.size() != 2) {
		throw SerializationException("Comparison needs two children!");
	}

	return make_unique<ComparisonExpression>(info->type, move(info->children[0]), move(info->children[1]));
}

ExpressionType ComparisonExpression::NegateComparisionExpression(ExpressionType type) {
	ExpressionType negated_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		negated_type = ExpressionType::COMPARE_NOTEQUAL;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		negated_type = ExpressionType::COMPARE_EQUAL;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_LESSTHAN;
		break;

	default:
		throw Exception("Unsupported join criteria in negation");
	}
	return negated_type;
}

ExpressionType ComparisonExpression::FlipComparisionExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
		flipped_type = type;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		flipped_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		flipped_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;

	default:
		throw Exception("Unsupported join criteria in flip");
	}
	return flipped_type;
}
