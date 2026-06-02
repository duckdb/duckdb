#pragma once
#include "duckdb/common/string.hpp"

namespace duckdb {
struct TriggerTableReferencingInfo {
	string new_table;
	string old_table;
};
} // namespace duckdb
