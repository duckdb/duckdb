#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

string TableFilter::DebugToString() const {
	return ToString("c0");
}

} // namespace duckdb
