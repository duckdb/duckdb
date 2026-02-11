#pragma once
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {
struct KeyActions {
	string update_action;
	string delete_action;
};
} // namespace duckdb
