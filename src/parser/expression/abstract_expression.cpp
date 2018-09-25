
#include "parser/expression/abstract_expression.hpp"

using namespace duckdb;
using namespace std;

bool AbstractExpression::IsAggregate() {
	bool is_aggregate = false;
	for (auto &child : children) {
		is_aggregate |= child->IsAggregate();
	}
	return is_aggregate;
}

bool AbstractExpression::IsScalar() {
	bool is_scalar = true;
	for (auto &child : children) {
		is_scalar &= child->IsScalar();
	}
	return is_scalar;
}

void AbstractExpression::GetAggregates(
    std::vector<AggregateExpression *> &expressions) {
	for (auto &child : children) {
		child->GetAggregates(expressions);
	}
}
