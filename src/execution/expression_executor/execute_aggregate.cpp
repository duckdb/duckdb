#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "execution/operator/aggregate/physical_aggregate.hpp"
#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(AggregateExpression &expr) {
	throw NotImplementedException("Execute aggregate expression");
}
