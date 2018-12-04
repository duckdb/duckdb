
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "execution/operator/physical_aggregate.hpp"
#include "parser/expression/groupref_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(GroupRefExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	vector.Reference(state->group_chunk.data[expr.group_index]);
	expr.stats.Verify(vector);
	return nullptr;
}
