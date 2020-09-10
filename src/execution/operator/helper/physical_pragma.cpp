#include "duckdb/execution/operator/helper/physical_pragma.hpp"

using namespace std;

namespace duckdb {

void PhysicalPragma::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &client = context.client;
	function.function(client, info.parameters);
}

} // namespace duckdb
