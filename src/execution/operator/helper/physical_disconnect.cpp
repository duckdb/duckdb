#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &client = context.client;
	if (!client.IsBoundToCatalog()) {
		throw InvalidInputException("DISCONNECT: no active CONNECT binding (already on LOCAL)");
	}
	client.UnbindCatalog();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
