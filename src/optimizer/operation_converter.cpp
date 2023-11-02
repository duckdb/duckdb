#include "duckdb/optimizer/operation_converter.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {


unique_ptr<LogicalOperator> OperationConverter::Optimize(unique_ptr<LogicalOperator> op) {
	return std::move(op);
}

} // namespace duckdb
