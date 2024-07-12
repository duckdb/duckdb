#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

case_insensitive_map_t<string> LogicalMaterializedCTE::ParamsToString() const {
	case_insensitive_map_t<string> result;
	result["Table Index"] = StringUtil::Format("%llu", table_index);
	return result;
}

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
