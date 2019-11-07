#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(BoundFunctionExpression &expr, Vector &result) {
	auto arguments = unique_ptr<Vector[]>(new Vector[expr.children.size()]);
	for (index_t i = 0; i < expr.children.size(); i++) {
		Execute(*expr.children[i], arguments[i]);
	}
	expr.function.function(*this, arguments.get(), expr.children.size(), expr, result);
	if (result.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, result.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}
