#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

namespace duckdb {

PhysicalVacuum::PhysicalVacuum(unique_ptr<VacuumInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::VACUUM, {LogicalType::BOOLEAN}, estimated_cardinality), info(move(info)) {
}

void PhysicalVacuum::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb
