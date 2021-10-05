#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

namespace duckdb {

void PhysicalVacuum::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb
