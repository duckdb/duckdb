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
	QueryProfiler() : automatic_printing(false), enabled(false) {
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

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk &chunk);

	string ToString() const;
	void Print();

	string ToJSON() const;
	void WriteJSONToFile(const char *path) const;

	bool automatic_printing;

private:
	bool enabled;

	unique_ptr<TreeNode> root;
	string query;

	Profiler main_query;
	Profiler op;
	std::unordered_map<PhysicalOperator *, TreeNode *> tree_map;
	std::stack<PhysicalOperator *> execution_stack;
};
} // namespace duckdb
