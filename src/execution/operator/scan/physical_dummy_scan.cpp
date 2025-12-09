#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

SourceResultType PhysicalDummyScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
