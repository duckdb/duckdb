#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "iostream"
namespace duckdb {

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto new_op_bindings = op->GetColumnBindings();
		std::cout << "in empty result, bindings are now " << std::endl;
		for (auto &bind : new_op_bindings) {
			std::cout << bind.table_index << ", " << bind.column_index << std::endl;
		}
	}
	this->bindings = op->GetColumnBindings();

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult() : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
}

} // namespace duckdb
