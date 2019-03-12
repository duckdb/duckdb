//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/profiler.hpp"
#include "common/string_util.hpp"
#include "common/types/data_chunk.hpp"

#include <stack>
#include <unordered_map>

namespace duckdb {
class PhysicalOperator;

enum class AutomaticPrintFormat : uint8_t { NONE, QUERY_TREE, JSON };

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	struct TimingInformation {
		double time = 0;
		size_t elements = 0;

		TimingInformation() : time(0), elements(0) {
		}
	};
	struct TreeNode {
		string name;
		string extra_info;
		vector<string> split_extra_info;
		TimingInformation info;
		vector<unique_ptr<TreeNode>> children;
		size_t depth = 0;
	};

private:
	static size_t GetDepth(QueryProfiler::TreeNode &node);
	unique_ptr<TreeNode> CreateTree(PhysicalOperator *root, size_t depth = 0);

	static size_t RenderTreeRecursive(TreeNode &node, vector<string> &render, vector<int> &render_heights,
	                                  size_t base_render_x = 0, size_t start_depth = 0, int depth = 0);
	static string RenderTree(TreeNode &node);

public:
	QueryProfiler() : automatic_print_format(AutomaticPrintFormat::NONE), enabled(false) {
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

	void StartQuery(string query);
	void EndQuery();

	void StartPhase(string phase);
	void EndPhase();

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk &chunk);

	string ToString() const;
	void Print();

	string ToJSON() const;
	void WriteToFile(const char *path, string& info) const;

	//! The format to automatically print query profiling information in (default: disabled)
	AutomaticPrintFormat automatic_print_format;
	//! The file to save query profiling information to, instead of printing it to the console (empty = print to console)
	string save_location;

private:
	//! Whether or not query profiling is enabled
	bool enabled;

	//! The root of the query tree
	unique_ptr<TreeNode> root;
	//! The query string 
	string query;

	//! The timer used to time the execution time of the entire query
	Profiler main_query;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! A map of a Physical Operator pointer to a tree node
	std::unordered_map<PhysicalOperator *, TreeNode *> tree_map;
	//! The stack of Physical Operators that are currently active
	std::stack<PhysicalOperator *> execution_stack;

	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	std::unordered_map<string, double> phase_timings;
	//! The stack of currently active phases
	std::vector<string> phase_stack;
};
} // namespace duckdb
