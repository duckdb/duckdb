#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &client = context.client;
	// Read is_bound so DISCONNECT still works when the target was detached elsewhere.
	if (!client.is_bound) {
		throw InvalidInputException("DISCONNECT: no active CONNECT binding (already on LOCAL)");
	}
	client.UnbindCatalog();
	return SourceResultType::FINISHED;
}

} // namespace duckdb
