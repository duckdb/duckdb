//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/helper/physical_explain.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalExplain represents the EXPLAIN operator
class PhysicalExplain : public PhysicalOperator {
  public:
	PhysicalExplain(LogicalOperator &op, vector<string> keys, vector<string> values)
	    : PhysicalOperator(PhysicalOperatorType::EXPLAIN, op.types), keys(keys), values(values) {
        assert(keys.size() == types.size());
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

    vector<string> keys;
    vector<string> values;
};

} // namespace duckdb
