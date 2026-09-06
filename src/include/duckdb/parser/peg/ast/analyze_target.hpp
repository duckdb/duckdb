#pragma once
#include "duckdb/common/common.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
struct AnalyzeTarget {
	unique_ptr<TableRef> ref;
	vector<Identifier> columns;
};
} // namespace duckdb
