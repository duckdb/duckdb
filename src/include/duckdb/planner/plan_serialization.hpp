//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/plan_serialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {
class ClientContext;
class LogicalOperator;

struct PlanDeserializationState {
	PlanDeserializationState(ClientContext &context) : context(context) {
	}

	ClientContext &context;
};

struct LogicalDeserializationState {
	LogicalDeserializationState(PlanDeserializationState &gstate, LogicalOperatorType type,
	                            vector<unique_ptr<LogicalOperator>> &children)
	    : gstate(gstate), type(type), children(children) {
	}

	PlanDeserializationState &gstate;
	LogicalOperatorType type;
	vector<unique_ptr<LogicalOperator>> &children;
};

struct ExpressionDeserializationState {
	ExpressionDeserializationState(PlanDeserializationState &gstate, ExpressionType type) : gstate(gstate), type(type) {
	}

	PlanDeserializationState &gstate;
	ExpressionType type;
};

} // namespace duckdb
