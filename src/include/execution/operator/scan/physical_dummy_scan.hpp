//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
public:
	PhysicalDummyScan(vector<TypeId> types) : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, types) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};
} // namespace duckdb
