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

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	struct TimingInformation {
		double time = 0;
		idx_t elements = 0;

		TimingInformation() : time(0), elements(0) {
		}
	};
	struct TreeNode {
		string name;
		string extra_info;
		vector<string> split_extra_info;
		TimingInformation info;
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

	void StartPhase(string phase);
	void EndPhase();

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk &chunk);

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
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! A map of a Physical Operator pointer to a tree node
	unordered_map<PhysicalOperator *, TreeNode *> tree_map;
	//! The stack of Physical Operators that are currently active
	std::stack<PhysicalOperator *> execution_stack;

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
