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
#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/main/profiler/profiler_print_format.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/main/profiler/profiling_utils.hpp"

namespace duckdb {

class BaseResultRenderer;
class ClientContext;
class ExpressionExecutor;
class ProfilingNode;
class PhysicalOperator;
class TreeRenderer;
class SQLStatement;
struct MetricsTimer;
class OperatorProfiler;

enum class ProfilingCoverage : uint8_t { SELECT = 0, ALL = 1 };

//! A JSON-like recursive profiling value.
//! FIXME: this should at some point be replaced by a "Value" - but that's not easily possible until our VARIANT Value
//! is extended to be able to easily hold arbitrary values
enum class QueryProfileResultKind {
	VALUE,
	LIST,
	OBJECT,
};

struct QueryProfileResult {
	QueryProfileResultKind kind = QueryProfileResultKind::OBJECT;
	//! Non-empty for children of an OBJECT node; empty for LIST items and the root
	string key;
	//! Valid when kind == VALUE
	Value value;
	//! Ordered children; valid when kind == LIST or OBJECT
	vector<unique_ptr<QueryProfileResult>> children;

	//! Add a named VALUE leaf to this OBJECT node
	void AddValue(const string &k, Value val);
	//! Add a named OBJECT child to this OBJECT node; returns the new child
	QueryProfileResult &AddObject(const string &k);
	//! Add a named LIST child to this OBJECT node; returns the new child
	QueryProfileResult &AddList(const string &k);
	//! Append an anonymous OBJECT item to this LIST node; returns the new item
	QueryProfileResult &AppendObject();
	//! Append an anonymous LIST item to this node; returns the new item
	QueryProfileResult &AppendList();
	//! Returns true if this node is a nested type (OBJECT or LIST)
	bool IsNested() const {
		return kind == QueryProfileResultKind::OBJECT || kind == QueryProfileResultKind::LIST;
	}
};

//! QueryProfiler collects the profiling metrics of a query.
class QueryProfiler {
public:
	using TreeMap = reference_map_t<const PhysicalOperator, reference<ProfilingNode>>;

public:
	DUCKDB_API explicit QueryProfiler(ClientContext &context);

public:
	DUCKDB_API bool IsEnabled() const;
	//! Create the TreeRenderer for the given profiler format name (e.g. "json", "query_tree"). Returns nullptr for the
	//! "no_output" format, and throws if the format name is not recognized.
	DUCKDB_API unique_ptr<TreeRenderer> CreateProfiler(const string &name) const;
	//! Returns the renderer to use for the given ProfilerPrintFormat, taking the configured default format into
	//! account.
	DUCKDB_API unique_ptr<TreeRenderer>
	GetRenderer(const ProfilerPrintFormat &format = ProfilerPrintFormat::Default()) const;
	DUCKDB_API bool PrintOptimizerOutput() const;
	DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void Start(const string &query);
	DUCKDB_API void Reset();
	DUCKDB_API void StartQuery(const string &query, bool is_explain_analyze = false, bool start_at_optimizer = false);
	DUCKDB_API void EndQuery();
	//! Finalize query metrics for output; safe to call multiple times.
	DUCKDB_API void FinalizeMetrics();

	//! Track bytes read (always tracked, even when profiling disabled).
	DUCKDB_API void TrackBytesRead(idx_t amount);
	//! Track bytes written (always tracked, even when profiling disabled).
	DUCKDB_API void TrackBytesWritten(idx_t amount);
	//! Track memory allocated (thread-safe; always tracked).
	DUCKDB_API void TrackTotalMemoryAllocated(idx_t amount);
	//! Add to a metric counter (profiling-only).
	DUCKDB_API void AddToMetricCounter(const string &key, idx_t amount);

	//! Set an arbitrary metric value (profiling-only; no-op when profiling is disabled).
	DUCKDB_API void SetMetric(const string &key, Value new_value);
	//! Returns true if the given metric is currently being tracked.
	//! Always returns false when profiling is disabled.
	DUCKDB_API bool MetricIsTracked(const string &key) const;

	//! Start a timer for a metric identified by its struct type.
	template <class METRIC>
	MetricsTimer StartTimer() {
		return StartTimerInternal(METRIC::Name);
	}
	//! Start a timer for a string-keyed metric (use the template overload when possible).
	DUCKDB_API MetricsTimer StartTimerInternal(const string &key);

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);
	//! Adds the top level query information to the global profiler.
	DUCKDB_API void SetBlockedTime(const double &blocked_thread_time);

	DUCKDB_API void Initialize(const PhysicalOperator &root);

	DUCKDB_API string QueryTreeToString() const;
	DUCKDB_API void QueryTreeToStream(std::ostream &str) const;
	//! Render the framed query tree (header, total time, phase timings, operator tree) into the given sink.
	DUCKDB_API void RenderQueryTree(BaseResultRenderer &ss) const;
	DUCKDB_API void Print();

	//! Render the profiler output as a string, formatted based on the given ProfilerPrintFormat (or the configured
	//! default profiler format when ProfilerPrintFormat::Default is passed).
	DUCKDB_API string ToString(const ProfilerPrintFormat &format = ProfilerPrintFormat::Default()) const;
	//! Render the profiler output for the given profiler format name (e.g. "json", "query_tree"), handling the
	//! profiling-disabled and no-output cases.
	DUCKDB_API string ToString(const string &profiler_format_name) const;
	//! Render the profiling node tree using the given renderer into the sink (renders nothing when there is no tree).
	//! Called by TreeRenderer::RenderProfiler for the formats that render the node tree directly.
	DUCKDB_API void RenderProfilingNodeTree(TreeRenderer &renderer, BaseResultRenderer &ss) const;

	// Sanitize a Value::MAP
	static Value JSONSanitize(const Value &input);
	static string JSONSanitize(const string &text);
	static string DrawPadded(const string &str, idx_t width);
	DUCKDB_API void ToLog() const;
	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;
	DUCKDB_API idx_t GetBytesRead() const;
	DUCKDB_API idx_t GetBytesWritten() const;

	//! Return the result tree (generating it if it does not yet exist)
	QueryProfileResult &GetResult();
	//! Returns true if the last query produced a profiling tree (i.e. profiling was enabled and the query succeeded)
	bool HasRoot() const;

private:
	unique_ptr<ProfilingNode> CreateTree(const PhysicalOperator &root, const idx_t depth = 0);
	void Render(const ProfilingNode &node, BaseResultRenderer &str) const;
	//! Render the profiler output to a string via the given renderer (nullptr renders nothing), handling the disabled
	//! case. Used for the programmatic / string paths.
	string RenderProfilerOutput(optional_ptr<TreeRenderer> renderer) const;
	//! Print the profiler output directly via the renderer's print sink (nullptr prints nothing), handling the
	//! disabled case. Only used on the terminal-print paths.
	void PrintProfilerOutput(optional_ptr<TreeRenderer> renderer) const;

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

	unique_ptr<GatheredMetrics> metrics;

	//! Top level query information.
	QueryMetrics query_metrics;

	unique_ptr<QueryProfileResult> result_tree;

	//! A map of a Physical Operator pointer to a tree node
	TreeMap tree_map;
	//! Whether or not we are running as part of a explain_analyze query
	bool is_explain_analyze;
	//! Whether root metrics have been finalized for output
	bool metrics_finalized;

public:
	const TreeMap &GetTreeMap() const {
		return tree_map;
	}

private:
	void FinalizeMetricsInternal();
	//! Write metrics to log without acquiring the lock (must be called with lock held).
	void ToLogInternal() const;

	unique_ptr<QueryProfileResult> ToResultTree() const;
	unique_ptr<QueryProfileResult> ToLegacyResultTree() const;

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(const PhysicalOperatorType op_type);
};

} // namespace duckdb
