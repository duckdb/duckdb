#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/common_subexpression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(CommonSubExpression &expr) {
	// check if the CSE has already been executed
	auto entry = state->cached_cse.find(expr.child);
	if (entry != state->cached_cse.end()) {
		// already existed, just reference the stored vector!
		vector.Reference(*(entry->second));
	} else {
		// else execute it
		expr.child->Accept(this);
		auto it = state->cached_cse.insert(make_pair(expr.child, make_unique<Vector>()));
		auto &inserted_vector = *(it.first->second);
		// move the result data to the vector cached in the CSE map
		vector.Move(inserted_vector);
		// now reference that vector in the result
		vector.Reference(inserted_vector);
	}
	return nullptr;
}
