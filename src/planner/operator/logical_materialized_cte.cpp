#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
