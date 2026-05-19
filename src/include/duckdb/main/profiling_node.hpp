//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiling_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/profiling_info.hpp"

namespace duckdb {

struct OperatorInformation {
	explicit OperatorInformation() {
		ResetMetrics();
	}

	string name;
	PhysicalOperatorType operator_type;

	double time;
	idx_t elements_returned;
	idx_t result_set_size;
	idx_t system_peak_buffer_manager_memory;
	idx_t system_peak_temp_directory_size;
	idx_t rows_scanned;

	InsertionOrderPreservingMap<string> extra_info;
	bool extra_info_dirty = false;

	profiler_metrics_t GetMetrics(const ProfilingInfo &info) const;
	void ResetMetrics() {
		time = 0;
		elements_returned = 0;
		result_set_size = 0;
		system_peak_buffer_manager_memory = 0;
		system_peak_temp_directory_size = 0;
		rows_scanned = 0;
		operator_type = PhysicalOperatorType::INVALID;
	}
	void GatherMetrics(ClientContext &context, double elapsed_time, optional_ptr<DataChunk> chunk);
	void Merge(const OperatorInformation &other);
};

//! The OperatorProfiler measures timings of individual operators
//! This class exists once for all operators and collects `OperatorInfo` for each operator
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API explicit OperatorProfiler(ClientContext &context);
	~OperatorProfiler() {
	}

public:
	DUCKDB_API void StartOperator(optional_ptr<const PhysicalOperator> phys_op);
	DUCKDB_API void EndOperator(optional_ptr<DataChunk> chunk);
	DUCKDB_API void FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate);

	//! Adds the timings in the OperatorProfiler (tree) to the QueryProfiler (tree).
	DUCKDB_API void Flush(const PhysicalOperator &phys_op);
	DUCKDB_API OperatorInformation &GetOperatorInfo(const PhysicalOperator &phys_op);
	DUCKDB_API bool OperatorInfoIsInitialized(const PhysicalOperator &phys_op);

public:
	ClientContext &context;

private:
	//! Whether or not the profiler is enabled
	bool enabled;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	optional_ptr<const PhysicalOperator> active_operator;
	//! A mapping of physical operators to profiled operator information.
	reference_map_t<const PhysicalOperator, OperatorInformation> operator_infos;
};

//! Recursive tree mirroring the operator tree.
class ProfilingNode {
public:
	explicit ProfilingNode() {
	}
	virtual ~ProfilingNode() {};

private:
	OperatorInformation operator_info;

public:
	idx_t depth = 0;
	vector<unique_ptr<ProfilingNode>> children;

public:
	idx_t GetChildCount() {
		return children.size();
	}
	OperatorInformation &GetOperatorInfo() {
		return operator_info;
	}
	const OperatorInformation &GetOperatorInfo() const {
		return operator_info;
	}
	optional_ptr<ProfilingNode> GetChild(idx_t idx) {
		return children[idx].get();
	}
	optional_ptr<ProfilingNode> AddChild(unique_ptr<ProfilingNode> child) {
		children.push_back(std::move(child));
		return children.back().get();
	}
};

} // namespace duckdb
