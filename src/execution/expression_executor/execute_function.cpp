
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "parser/expression/function_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(FunctionExpression &expr) {
	assert(expr.bound_function);

	auto arguments = unique_ptr<Vector[]>(new Vector[expr.children.size()]);
	for (size_t i = 0; i < expr.children.size(); i++) {
		expr.children[i]->Accept(this);
		vector.Move(arguments[i]);
	}
	vector.Destroy();
	expr.bound_function->function(arguments.get(), expr.children.size(),
	                              vector);
	if (vector.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, vector.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
	return nullptr;
}