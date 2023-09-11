#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {

	this->bindings = op->GetColumnBindings();

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult() : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
}

} // namespace duckdb
