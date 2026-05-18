#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	if (info->target_is_local) {
		throw NotImplementedException("DISCONNECT LOCAL is not yet implemented");
	}
	if (info->name.empty()) {
		throw NotImplementedException("DISCONNECT with no target is not yet implemented");
	}
	if (info->name_is_string_literal) {
		throw NotImplementedException("DISCONNECT '<connection_string>' is not yet implemented");
	}
	throw NotImplementedException("DISCONNECT <name> is not yet implemented");
}

} // namespace duckdb
