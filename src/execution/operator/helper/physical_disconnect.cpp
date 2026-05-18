#include "duckdb/execution/operator/helper/physical_disconnect.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

SourceResultType PhysicalDisconnect::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	throw NotImplementedException("DISCONNECT is not yet implemented");
}

} // namespace duckdb
