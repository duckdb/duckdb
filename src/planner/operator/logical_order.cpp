#include "planner/operator/logical_order.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalOrder::GetNames() {
	return children[0]->GetNames();
}

void LogicalOrder::ResolveTypes() {
	types = children[0]->types;
}

size_t LogicalOrder::ExpressionCount() {
	assert(expressions.size() == 0);
	return description.orders.size();
}

Expression *LogicalOrder::GetExpression(size_t index) {
	assert(index < description.orders.size());
	return description.orders[index].expression.get();
}

void LogicalOrder::ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                     size_t index) {
	assert(index < description.orders.size());
	description.orders[index].expression = callback(move(description.orders[index].expression));
}
