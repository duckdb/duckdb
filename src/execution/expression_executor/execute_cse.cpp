#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Execute(CommonSubExpression &expr, Vector &result) {
	// check if the CSE has already been executed
	auto entry = cached_cse.find(expr.child);
	if (entry != cached_cse.end()) {
		// already existed, just reference the stored vector!
		result.Reference(*(entry->second));
	} else {
		// else execute it
		Execute(*expr.child, result);
		auto it = cached_cse.insert(make_pair(expr.child, make_unique<Vector>()));
		auto &inserted_vector = *(it.first->second);
		// move the result data to the vector cached in the CSE map
		result.Move(inserted_vector);
		// now reference that vector in the result
		result.Reference(inserted_vector);
	}
}
