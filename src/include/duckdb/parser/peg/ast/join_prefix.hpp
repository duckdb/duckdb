#pragma once
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {
struct JoinPrefix {
	JoinRefType ref_type;
	JoinType join_type = JoinType::INNER; // Inner is default, unless otherwise explicitly set
};
} // namespace duckdb
