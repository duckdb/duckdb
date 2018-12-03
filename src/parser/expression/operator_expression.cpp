
#include "parser/expression/operator_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void OperatorExpression::ResolveType() {
	Expression::ResolveType();
	// logical operators return a bool
	if (type == ExpressionType::OPERATOR_NOT ||
	    type == ExpressionType::OPERATOR_IS_NULL ||
	    type == ExpressionType::OPERATOR_IS_NOT_NULL ||
	    type == ExpressionType::OPERATOR_EXISTS ||
	    type == ExpressionType::OPERATOR_NOT_EXISTS ||
	    type == ExpressionType::COMPARE_IN ||
	    type == ExpressionType::COMPARE_NOT_IN) {
		return_type = TypeId::BOOLEAN;
		return;
	}
	return_type = std::max(children[0]->return_type, children[1]->return_type);
	switch (type) {
	case ExpressionType::OPERATOR_ADD:
		ExpressionStatistics::Add(children[0]->stats, children[1]->stats,
		                          stats);
		break;
	case ExpressionType::OPERATOR_SUBTRACT:
		ExpressionStatistics::Subtract(children[0]->stats, children[1]->stats,
		                               stats);
		break;
	case ExpressionType::OPERATOR_MULTIPLY:
		ExpressionStatistics::Multiply(children[0]->stats, children[1]->stats,
		                               stats);
		break;
	case ExpressionType::OPERATOR_DIVIDE:
		ExpressionStatistics::Divide(children[0]->stats, children[1]->stats,
		                             stats);
		break;
	case ExpressionType::OPERATOR_MOD:
		ExpressionStatistics::Modulo(children[0]->stats, children[1]->stats,
		                             stats);
		break;
	default:
		throw NotImplementedException(
		    "Unsupported operator type for statistics!");
	}
	// return the highest type of the children, unless we need to upcast to
	// avoid overflow
	if (!stats.FitsInType(return_type)) {
		return_type = stats.MinimalType();
	}
}

unique_ptr<Expression> OperatorExpression::Copy() {
	auto copy = make_unique<OperatorExpression>(type, return_type);
	copy->CopyProperties(*this);
	copy->CopyChildren(*this);
	return copy;
}

unique_ptr<Expression>
OperatorExpression::Deserialize(ExpressionDeserializeInformation *info,
                                Deserializer &source) {
	auto expression =
	    make_unique<OperatorExpression>(info->type, info->return_type);
	expression->children = move(info->children);
	return expression;
}
