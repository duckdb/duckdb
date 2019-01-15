#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Visit(BoundFunctionExpression &expr) {
	assert(expr.bound_function);

	auto arguments = unique_ptr<Vector[]>(new Vector[expr.function->children.size()]);
	for (size_t i = 0; i < expr.function->children.size(); i++) {
		Execute(expr.function->children[i]);
		vector.Move(arguments[i]);
	}
	vector.Destroy();
	expr.bound_function->function(arguments.get(), expr.function->children.size(), expr, vector);
	if (vector.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, vector.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
	Verify(expr);
}
