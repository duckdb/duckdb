#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
	this->bindings = op->GetColumnBindings();

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult(vector<LogicalType> return_types_p, vector<ColumnBinding> bindings_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT), return_types(std::move(return_types_p)),
      bindings(std::move(bindings_p)) {
}

LogicalEmptyResult::LogicalEmptyResult() : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
}

} // namespace duckdb
