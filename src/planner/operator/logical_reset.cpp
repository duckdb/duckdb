#include "duckdb/planner/operator/logical_reset.hpp"

namespace duckdb {
class ClientContext;

idx_t LogicalReset::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
