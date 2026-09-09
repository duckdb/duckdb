#pragma once
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {
struct KeyActions {
	string update_action;
	string delete_action;
};
} // namespace duckdb
