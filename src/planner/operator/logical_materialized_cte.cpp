#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

InsertionOrderPreservingMap<string> LogicalMaterializedCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Index"] = StringUtil::Format("%llu", table_index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
