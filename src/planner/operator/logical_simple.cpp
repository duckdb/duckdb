#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {
class ClientContext;

idx_t LogicalSimple::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
