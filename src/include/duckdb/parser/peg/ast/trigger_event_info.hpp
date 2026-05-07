#pragma once
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
struct TriggerEventInfo {
	TriggerEventType event_type;
	vector<string> columns;
};
} // namespace duckdb
