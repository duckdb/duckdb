#include "duckdb/execution/operator/helper/physical_pragma.hpp"

namespace duckdb {

void PhysicalPragma::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate) const {
	auto &client = context.client;
	FunctionParameters parameters {info.parameters, info.named_parameters};
	function.function(client, parameters);
}

} // namespace duckdb
