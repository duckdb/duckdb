//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_extension.operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {

struct LogicalExtensionOperator : LogicalOperator {
	OperatorExtensionInfo *operator_info;

	LogicalExtensionOperator(OperatorExtensionInfo *info)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR), operator_info(info) {
	}
	LogicalExtensionOperator(OperatorExtensionInfo *info, vector<unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR, move(expressions)), operator_info(info) {
	}
};
} // namespace duckdb
