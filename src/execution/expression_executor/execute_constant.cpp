#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(ConstantExpression &expr) {
	vector.Reference(expr.value);
	expr.stats.Verify(vector);
	return nullptr;
}
