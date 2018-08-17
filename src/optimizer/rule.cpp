#include "optimizer/rule.hpp"
using namespace duckdb;

AbstractOperator::iterator AbstractOperator::begin() {
	assert(type == AbstractOperatorType::LOGICAL_OPERATOR);
	return iterator(value.op);
}

AbstractOperator::iterator AbstractOperator::end() {
	assert(type == AbstractOperatorType::LOGICAL_OPERATOR);
	return iterator(value.op, value.op->children.size(),
	                value.op->ExpressionCount());
}
