#include "duckdb/execution/operator/helper/physical_connect.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

SourceResultType PhysicalConnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	if (info->target_is_local) {
		throw NotImplementedException("CONNECT LOCAL is not yet implemented");
	}
	if (info->name.empty()) {
		throw NotImplementedException("CONNECT with no target is not yet implemented");
	}
	if (info->name_is_string_literal) {
		throw NotImplementedException("CONNECT '<connection_string>' is not yet implemented");
	}
	throw NotImplementedException("CONNECT <name> is not yet implemented");
}

} // namespace duckdb
