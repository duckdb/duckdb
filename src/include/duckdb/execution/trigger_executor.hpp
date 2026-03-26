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
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class TableCatalogEntry;

struct TriggerInfo {
	unique_ptr<QueryNode> body;
	TriggerForEach for_each;
};

class TriggerExecutor {
public:
	// Each recursive trigger level pushes Parser, Planner, Optimizer, and PlanGenerator onto the call stack.
	// Keep depth small enough that stack overflow cannot occur before the guard fires.
	static constexpr idx_t MAX_TRIGGER_DEPTH = 8;

	//! Fire pre-collected triggers with the given row count.
	// TODO - probably going to change to receive a chunk to support NEW/OLD and REFERENCING NEW TABLE AS
	static void Fire(ClientContext &context, const vector<TriggerInfo> &triggers, idx_t row_count);
};
} // namespace duckdb
