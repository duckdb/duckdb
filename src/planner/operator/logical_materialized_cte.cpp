#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

InsertionOrderPreservingMap<string> LogicalMaterializedCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetParamsEstimatedCardinality(result);
	return result;
}

vector<TableIndex> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

} // namespace duckdb
