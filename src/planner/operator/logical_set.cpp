#include "duckdb/planner/operator/logical_set.hpp"

namespace duckdb {

idx_t LogicalSet::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
