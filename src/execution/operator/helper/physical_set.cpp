#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalSet::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const {
	auto &db = context.client.db;
	db->config.set_variables[name] = value; // woop
}

} // namespace duckdb
