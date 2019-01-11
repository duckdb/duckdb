#include "planner/operator/logical_table_function.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalTableFunction::GetNames() {
	vector<string> names;
	for (auto &column : function->return_values) {
		names.push_back(column.name);
	}
	return names;
}

void LogicalTableFunction::ResolveTypes() {
	for (auto &column : function->return_values) {
		types.push_back(column.type);
	}
}

size_t LogicalTableFunction::ExpressionCount() {
	assert(expressions.size() == 0);
	return 1;
}

Expression *LogicalTableFunction::GetExpression(size_t index) {
	assert(index == 0);
	return function_call.get();
}

void LogicalTableFunction::ReplaceExpression(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, size_t index) {
	assert(index == 0);
	function_call = callback(move(function_call));
}
