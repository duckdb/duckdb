#include "duckdb/execution/operator/helper/physical_prepare.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

SourceResultType PhysicalPrepare::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &client = context.client;

	// store the prepared statement in the context
	ClientData::Get(client).prepared_statements[name.ToStdString()] = prepared;

	return SourceResultType::FINISHED;
}

} // namespace duckdb
