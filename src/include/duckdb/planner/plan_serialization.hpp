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

struct PlanDeserializationState {
	PlanDeserializationState(ClientContext &context) : context(context) {
	}

	ClientContext &context;
};

struct LogicalDeserializationState {
	LogicalDeserializationState(PlanDeserializationState &gstate, LogicalOperatorType type)
	    : gstate(gstate), type(type) {
	}

	PlanDeserializationState &gstate;
	LogicalOperatorType type;
};

struct ExpressionDeserializationState {
	ExpressionDeserializationState(PlanDeserializationState &gstate, ExpressionType type) : gstate(gstate), type(type) {
	}

	PlanDeserializationState &gstate;
	ExpressionType type;
};

} // namespace duckdb
