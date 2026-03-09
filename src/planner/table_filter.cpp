#include "duckdb/planner/table_filter.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

string TableFilter::DebugToString() const {
	return ToString("c0");
}

} // namespace duckdb
