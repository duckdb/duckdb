#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &client = context.client;
	// IsConnected() so DISCONNECT still works when the target was detached elsewhere.
	if (!client.IsConnected()) {
		throw InvalidInputException("DISCONNECT: no active CONNECT (already on LOCAL)");
	}
	client.DisconnectFromCatalog();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
