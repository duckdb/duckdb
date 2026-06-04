#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

struct DescribeTarget {
	bool is_table_name = false;
	string table_name;
	unique_ptr<BaseTableRef> table_ref;
};

} // namespace duckdb
