//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/profiler_format.hpp"

#include <stack>
#include <unordered_map>

namespace duckdb {
class PhysicalOperator;
class SQLStatement;

struct OperatorTimingInformation {
	double time = 0;
	idx_t elements = 0;

	OperatorTimingInformation(double time_ = 0, idx_t elements_ = 0) : time(time_), elements(elements_) {
	}
};

//! The OperatorProfiler measures timings of individual operators
class OperatorProfiler {
	friend class QueryProfiler;

public:
	OperatorProfiler(bool enabled);

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk *chunk);

private:
	void AddTiming(PhysicalOperator *op, double time, idx_t elements);

	//! Whether or not the profiler is enabled
	bool enabled;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	std::stack<PhysicalOperator *> execution_stack;
	//! A mapping of physical operators to recorded timings
	unordered_map<PhysicalOperator *, OperatorTimingInformation> timings;
};

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	struct TreeNode {
		string name;
		string extra_info;
		vector<string> split_extra_info;
		OperatorTimingInformation info;
		vector<unique_ptr<TreeNode>> children;
		idx_t depth = 0;
	};

private:
	static idx_t GetDepth(QueryProfiler::TreeNode &node);
	unique_ptr<TreeNode> CreateTree(PhysicalOperator *root, idx_t depth = 0);

	static idx_t RenderTreeRecursive(TreeNode &node, vector<string> &render, vector<idx_t> &render_heights,
	                                 idx_t base_render_x = 0, idx_t start_depth = 0, idx_t depth = 0);
	static string RenderTree(TreeNode &node);

public:
	QueryProfiler() : automatic_print_format(ProfilerPrintFormat::NONE), enabled(false), running(false) {
	}

	void Enable() {
		enabled = true;
	}

	void Disable() {
		enabled = false;
	}

	bool IsEnabled() {
		return enabled;
	}

	void StartQuery(string query, SQLStatement &statement);
	void EndQuery();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	void Flush(OperatorProfiler &profiler);

	void StartPhase(string phase);
	void EndPhase();

	void Initialize(PhysicalOperator *root);

	string ToString() const;
	void Print();

	string ToJSON() const;
	void WriteToFile(const char *path, string &info) const;

	//! The format to automatically print query profiling information in (default: disabled)
	ProfilerPrintFormat automatic_print_format;
	//! The file to save query profiling information to, instead of printing it to the console (empty = print to
	//! console)
	string save_location;

private:
	//! Whether or not query profiling is enabled
	bool enabled;
	//! Whether or not the query profiler is running
	bool running;

	//! The root of the query tree
	unique_ptr<TreeNode> root;
	//! The query string
	string query;

	//! The timer used to time the execution time of the entire query
	Profiler main_query;
	//! A map of a Physical Operator pointer to a tree node
	unordered_map<PhysicalOperator *, TreeNode *> tree_map;

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
};
} // namespace duckdb
