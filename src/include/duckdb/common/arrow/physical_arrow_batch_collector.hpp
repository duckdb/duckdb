#pragma once

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"

namespace duckdb {

class ArrowBatchGlobalState : public BatchCollectorGlobalState {
public:
	ArrowBatchGlobalState(ClientContext &context, const PhysicalBatchCollector &op)
	    : BatchCollectorGlobalState(context, op) {
	}
};

class PhysicalArrowBatchCollector : public PhysicalBatchCollector {
public:
	PhysicalArrowBatchCollector(PreparedStatementData &data, idx_t batch_size)
	    : PhysicalBatchCollector(data), record_batch_size(batch_size) {
	}

public:
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

public:
	//! User provided batch size
	idx_t record_batch_size;
};

} // namespace duckdb
