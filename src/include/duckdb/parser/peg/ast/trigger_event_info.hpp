#pragma once
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/vector.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
struct TriggerEventInfo {
	TriggerEventType event_type;
	vector<Identifier> columns;
};
} // namespace duckdb
