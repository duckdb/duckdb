#pragma once

#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"

namespace duckdb {

class NumpyBatchGlobalState : public BatchCollectorGlobalState {
public:
	NumpyBatchGlobalState(ClientContext &context, const PhysicalBatchCollector &op)
	    : BatchCollectorGlobalState(context, op) {
	}
	~NumpyBatchGlobalState() override {
		if (!py::gil_check()) {
			// If an exception occurred, we need to grab the gil so we can destroy this
			py::gil_scoped_acquire gil;
			result.reset();
			return;
		}
		result.reset();
	}
};

class PhysicalNumpyBatchCollector : public PhysicalBatchCollector {
public:
	PhysicalNumpyBatchCollector(PreparedStatementData &data) : PhysicalBatchCollector(data) {
	}

public:
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
};

} // namespace duckdb
