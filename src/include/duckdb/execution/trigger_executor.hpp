//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/trigger_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/trigger_type.hpp"

namespace duckdb {
class ClientContext;
class TableCatalogEntry;

class TriggerExecutor {
public:
	// Each recursive trigger level pushes Parser, Planner, Optimizer, and PlanGenerator onto the
	// call stack. Keep depth small enough that stack overflow cannot occur before the guard fires.
	static constexpr idx_t MAX_TRIGGER_DEPTH = 8;

	static void Fire(ClientContext &context, TableCatalogEntry &table, idx_t row_count, TriggerTiming timing,
	                 TriggerEventType event_type);
};
} // namespace duckdb
