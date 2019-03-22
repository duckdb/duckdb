#include "planner/operator/logical_order.hpp"

using namespace duckdb;
using namespace std;

size_t LogicalOrder::ExpressionCount() {
	assert(expressions.size() == 0);
	return orders.size();
}

Expression *LogicalOrder::GetExpression(size_t index) {
	assert(index < orders.size());
	return orders[index].expression.get();
}

void LogicalOrder::ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                     size_t index) {
	assert(index < orders.size());
	orders[index].expression = callback(move(orders[index].expression));
}
