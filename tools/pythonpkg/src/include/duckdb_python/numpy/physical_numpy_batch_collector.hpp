#pragma once

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"

namespace duckdb {

class PhysicalNumpyBatchCollector : public PhysicalBatchCollector {
public:
	PhysicalNumpyBatchCollector(PreparedStatementData &data) : PhysicalBatchCollector(data) {
	}

public:
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;
};

} // namespace duckdb
