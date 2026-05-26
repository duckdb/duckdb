#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &client = context.client;
	// Read is_bound directly: this lets DISCONNECT succeed even when the target was detached
	// out from under us (TryGetBoundCatalog() would return nullptr there). The user still
	// needs a way to clear the broken binding.
	if (!client.is_bound) {
		throw InvalidInputException("DISCONNECT: no active CONNECT binding (already on LOCAL)");
	}
	client.UnbindCatalog();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
