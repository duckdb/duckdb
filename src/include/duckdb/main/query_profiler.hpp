//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/profiling_info.hpp"
#include "duckdb/main/profiling_node.hpp"

#include <stack>

namespace duckdb {

class ClientContext;
class ExpressionExecutor;
class ProfilingNode;
class PhysicalOperator;
class SQLStatement;

enum class ProfilingCoverage : uint8_t { SELECT = 0, ALL = 1 };

struct OperatorInformation {
	explicit OperatorInformation() {
	}

	string name;

	double time = 0;
	idx_t elements_returned = 0;
	idx_t result_set_size = 0;
	idx_t system_peak_buffer_manager_memory = 0;
	idx_t system_peak_temp_directory_size = 0;

	InsertionOrderPreservingMap<string> extra_info;

	void AddTime(double n_time) {
		time += n_time;
	}

	void AddReturnedElements(idx_t n_elements) {
		elements_returned += n_elements;
	}

	void AddResultSetSize(idx_t n_result_set_size) {
		result_set_size += n_result_set_size;
	}

	void UpdateSystemPeakBufferManagerMemory(idx_t used_memory) {
		if (used_memory > system_peak_buffer_manager_memory) {
			system_peak_buffer_manager_memory = used_memory;
		}
	}

	void UpdateSystemPeakTempDirectorySize(idx_t used_swap) {
		if (used_swap > system_peak_temp_directory_size) {
			system_peak_temp_directory_size = used_swap;
		}
	}
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
	//! Sub-settings for the operator profiler
	profiler_settings_t settings;

	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	optional_ptr<const PhysicalOperator> active_operator;
	//! A mapping of physical operators to profiled operator information.
	reference_map_t<const PhysicalOperator, OperatorInformation> operator_infos;
};

//! Top level query metrics.
struct QueryMetrics {
	QueryMetrics() : total_bytes_read(0), total_bytes_written(0), total_memory_allocated(0) {};

	//! Reset the query metrics.
	void Reset() {
		query = "";
		latency.Reset();
		waiting_to_attach_latency.Reset();
		attach_load_storage_latency.Reset();
		attach_replay_wal_latency.Reset();
		checkpoint_latency.Reset();
		commit_local_storage_latency.Reset();
		write_to_wal_latency.Reset();
		wal_replay_entry_count = 0;
		total_bytes_read = 0;
		total_bytes_written = 0;
		total_memory_allocated = 0;
	}

	ProfilingInfo query_global_info;

	//! The SQL string of the query.
	string query;
	//! The timer of the execution of the entire query.
	Profiler latency;
	//! The timer of the delay when waiting to ATTACH a file.
	Profiler waiting_to_attach_latency;
	//! The timer for loading from storage.
	Profiler attach_load_storage_latency;
	//! The timer for replaying the WAL file.
	Profiler attach_replay_wal_latency;
	//! The timer for running checkpoints.
	Profiler checkpoint_latency;
	//! The timer for committing the transaction-local storage.
	Profiler commit_local_storage_latency;
	//! The timer for the WAL writes.
	Profiler write_to_wal_latency;
	//! The total number of entries to replay in the WAL.
	atomic<idx_t> wal_replay_entry_count;
	//! The total bytes read by the file system.
	atomic<idx_t> total_bytes_read;
	//! The total bytes written by the file system.
	atomic<idx_t> total_bytes_written;
	//! The total memory allocated by the buffer manager.
	atomic<idx_t> total_memory_allocated;
};

//! QueryProfiler collects the profiling metrics of a query.
class QueryProfiler {
public:
	using TreeMap = reference_map_t<const PhysicalOperator, reference<ProfilingNode>>;

public:
	DUCKDB_API explicit QueryProfiler(ClientContext &context);

public:
	DUCKDB_API bool IsEnabled() const;
	DUCKDB_API bool IsDetailedEnabled() const;
	DUCKDB_API ProfilerPrintFormat GetPrintFormat(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API bool PrintOptimizerOutput() const;
	DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void Start(const string &query);
	DUCKDB_API void Reset();
	DUCKDB_API void StartQuery(const string &query, bool is_explain_analyze = false, bool start_at_optimizer = false);
	DUCKDB_API void EndQuery();

	//! Adds amount to a specific metric type.
	DUCKDB_API void AddToCounter(MetricsType type, const idx_t amount);

	//! Start/End a timer for a specific metric type.
	DUCKDB_API void StartTimer(MetricsType type);
	DUCKDB_API void EndTimer(MetricsType type);

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);
	//! Adds the top level query information to the global profiler.
	DUCKDB_API void SetBlockedTime(const double &blocked_thread_time);

	DUCKDB_API void StartPhase(MetricsType phase_metric);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(const PhysicalOperator &root);

	DUCKDB_API string QueryTreeToString() const;
	DUCKDB_API void QueryTreeToStream(std::ostream &str) const;
	DUCKDB_API void Print();

	//! return the printed as a string. Unlike ToString, which is always formatted as a string,
	//! the return value is formatted based on the current print format (see GetPrintFormat()).
	DUCKDB_API string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API string ToString(ProfilerPrintFormat format) const;

	// Sanitize a Value::MAP
	static Value JSONSanitize(const Value &input);
	static string JSONSanitize(const string &text);
	static string DrawPadded(const string &str, idx_t width);
	DUCKDB_API void ToLog() const;
	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;
	DUCKDB_API idx_t GetBytesRead() const;
	DUCKDB_API idx_t GetBytesWritten() const;

	idx_t OperatorSize() {
		return tree_map.size();
	}

	void Finalize(ProfilingNode &node);

	//! Return the root of the query tree.
	optional_ptr<ProfilingNode> GetRoot() {
		return root.get();
	}

	//! Provides access to the root of the query tree, but ensures there are no concurrent modifications.
	//! This can be useful when implementing continuous profiling or making customizations.
	DUCKDB_API void GetRootUnderLock(const std::function<void(optional_ptr<ProfilingNode>)> &callback) {
		lock_guard<std::mutex> guard(lock);
		callback(GetRoot());
	}

private:
	unique_ptr<ProfilingNode> CreateTree(const PhysicalOperator &root, const profiler_settings_t &settings,
	                                     const idx_t depth = 0);
	void Render(const ProfilingNode &node, std::ostream &str) const;
	string RenderDisabledMessage(ProfilerPrintFormat format) const;

private:
	ClientContext &context;

	//! Whether or not the query profiler is running
	bool running;
	//! The lock used for accessing the global query profiler or flushing information to it from a thread
	mutable std::mutex lock;

	//! Whether or not the query requires profiling
	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<ProfilingNode> root;

	//! Top level query information.
	QueryMetrics query_metrics;

	//! A map of a Physical Operator pointer to a tree node
	TreeMap tree_map;
	//! Whether or not we are running as part of a explain_analyze query
	bool is_explain_analyze;

public:
	const TreeMap &GetTreeMap() const {
		return tree_map;
	}

private:
	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<MetricsType, double, MetricsTypeHashFunction>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<MetricsType> phase_stack;

private:
	void MoveOptimizerPhasesToRoot();

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(const PhysicalOperatorType op_type);
	ExplainFormat GetExplainFormat(ProfilerPrintFormat format) const;
};

} // namespace duckdb
