#include "duckdb/execution/operator/helper/physical_create_secret.hpp"

namespace duckdb {

SourceResultType PhysicalCreateSecret::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &client = context.client;
	function.function(client, info.named_parameters);

	// TODO return stuff?

	return SourceResultType::FINISHED;
}

} // namespace duckdb
