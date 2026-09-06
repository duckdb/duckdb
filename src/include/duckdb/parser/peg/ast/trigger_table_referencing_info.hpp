#pragma once
#include "duckdb/common/string.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
struct TriggerTableReferencingInfo {
	Identifier new_table;
	Identifier old_table;
};
} // namespace duckdb
