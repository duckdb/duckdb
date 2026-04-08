#include "duckdb/planner/operator/logical_prepare.hpp"

namespace duckdb {
class ClientContext;

idx_t LogicalPrepare::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
