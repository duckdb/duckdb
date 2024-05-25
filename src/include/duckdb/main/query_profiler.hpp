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

#include <stack>

namespace duckdb {
class ClientContext;
class ExpressionExecutor;
class PhysicalOperator;
class SQLStatement;

struct OperatorInformation {
	explicit OperatorInformation(double time_p = 0, idx_t elements_p = 0) : time(time_p), elements(elements_p) {
	}

	double time;
	idx_t elements;
	string name;

	void AddTime(double n_time) {
		this->time += n_time;
	}

	void AddElements(idx_t n_elements) {
		this->elements += n_elements;
	}
};

//! The OperatorProfiler measures timings of individual operators
//! This class exists once for all operators and collects `OperatorInfo` for each operator
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API explicit OperatorProfiler(ClientContext &context);

	DUCKDB_API void StartOperator(optional_ptr<const PhysicalOperator> phys_op);
	DUCKDB_API void EndOperator(optional_ptr<DataChunk> chunk);

	//! Adds the timings gathered in the OperatorProfiler (tree) to the QueryProfiler (tree)
	DUCKDB_API void Flush(const PhysicalOperator &phys_op, ExpressionExecutor &expression_executor, const string &name,
	                      int id);
	DUCKDB_API OperatorInformation &GetOperatorInfo(const PhysicalOperator &phys_op);

	static bool SettingEnabled(const MetricsType setting) {
		return SettingSetFunctions::Enabled(ProfilingInfo::DefaultSettings(), setting);
	}

	~OperatorProfiler() {
	}

private:
	//! Whether or not the profiler is enabled
	bool enabled;
	profiler_settings_t settings;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	optional_ptr<const PhysicalOperator> active_operator;
	//! A mapping of physical operators to recorded timings
	reference_map_t<const PhysicalOperator, OperatorInformation> timings;
};

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	DUCKDB_API explicit QueryProfiler(ClientContext &context);

public:
	// Recursive tree that mirrors the operator tree
	struct TreeNode {
		PhysicalOperatorType type;
		string name;
		ProfilingInfo profiling_info;
		vector<unique_ptr<TreeNode>> children;
		idx_t depth = 0;
	};

	// Holds the top level query info
	struct QueryInfo {
		string query;
		ProfilingInfo settings;
	};

	// Propagate save_location, enabled, detailed_enabled and automatic_print_format.
	void Propagate(QueryProfiler &qp);

	using TreeMap = reference_map_t<const PhysicalOperator, reference<TreeNode>>;

private:
	unique_ptr<TreeNode> CreateTree(const PhysicalOperator &root, profiler_settings_t settings, idx_t depth = 0);
	void Render(const TreeNode &node, std::ostream &str) const;

public:
	DUCKDB_API bool IsEnabled() const;
	DUCKDB_API bool IsDetailedEnabled() const;
	DUCKDB_API ProfilerPrintFormat GetPrintFormat() const;
	DUCKDB_API bool PrintOptimizerOutput() const;
	DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void StartQuery(string query, bool is_explain_analyze = false, bool start_at_optimizer = false);
	DUCKDB_API void EndQuery();

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);

	DUCKDB_API void StartPhase(string phase);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(const PhysicalOperator &root);

	DUCKDB_API string QueryTreeToString() const;
	DUCKDB_API void QueryTreeToStream(std::ostream &str) const;
	DUCKDB_API void Print();

	//! return the printed as a string. Unlike ToString, which is always formatted as a string,
	//! the return value is formatted based on the current print format (see GetPrintFormat()).
	DUCKDB_API string ToString() const;

	static string JSONSanitize(const string &text);
	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;

	idx_t OperatorSize() {
		return tree_map.size();
	}

	void Finalize(TreeNode &node);

private:
	ClientContext &context;

	//! Whether or not the query profiler is running
	bool running;
	//! The lock used for flushing information from a thread into the global query profiler
	mutex flush_lock;

	//! Whether or not the query requires profiling
	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<TreeNode> root;

	//! The query info
	unique_ptr<QueryInfo> query_info;

	//! The query string
	string query;
	//! The timer used to time the execution time of the entire query
	Profiler main_query;
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
	using PhaseTimingStorage = unordered_map<string, double>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<string> phase_stack;

private:
	vector<PhaseTimingItem> GetOrderedPhaseTimings() const;

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(PhysicalOperatorType op_type);
	void ReadAndSetCustomProfilerSettings(const string &settings_path);
};

} // namespace duckdb
