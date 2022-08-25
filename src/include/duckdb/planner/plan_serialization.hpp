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
#include "duckdb/planner/bound_parameter_map.hpp"

namespace duckdb {
class ClientContext;
class LogicalOperator;
struct BoundParameterData;

struct PlanDeserializationState {
	PlanDeserializationState(ClientContext &context);
	~PlanDeserializationState();

	ClientContext &context;
	bound_parameter_map_t parameter_data;
};

struct LogicalDeserializationState {
	LogicalDeserializationState(PlanDeserializationState &gstate, LogicalOperatorType type,
	                            vector<unique_ptr<LogicalOperator>> &children);

	PlanDeserializationState &gstate;
	LogicalOperatorType type;
	vector<unique_ptr<LogicalOperator>> &children;
};

struct ExpressionDeserializationState {
	ExpressionDeserializationState(PlanDeserializationState &gstate, ExpressionType type);

	PlanDeserializationState &gstate;
	ExpressionType type;
};

} // namespace duckdb
