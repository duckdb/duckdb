//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/trigger_body_transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

static constexpr const char *TRIGGER_BASE_CTE_NAME = "__duckdb_trigger_base";

void TransformTriggerBody(QueryNode &body, TriggerEventType event_type);

} // namespace duckdb
