#pragma once
#include "duckdb/common/common.hpp"

namespace duckdb {
struct AnalyzeTarget {
	unique_ptr<TableRef> ref;
	vector<string> columns;
};
} // namespace duckdb
