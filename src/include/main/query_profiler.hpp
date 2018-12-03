//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// main/query_profiler.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <stack>
#include <unordered_map>

#include "common/common.hpp"
#include "common/printable.hpp"
#include "common/profiler.hpp"
#include "common/string_util.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {
class PhysicalOperator;

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler : public Printable {
  public:
	struct TimingInformation {
		double time = 0;
		size_t elements = 0;

		TimingInformation() : time(0), elements(0) {
		}
	};
	struct TreeNode {
		std::string name;
		std::vector<std::string> extra_info;
		TimingInformation info;
		std::vector<std::unique_ptr<TreeNode>> children;
		size_t depth = 0;
	};

  private:
	static size_t GetDepth(QueryProfiler::TreeNode &node);
	std::unique_ptr<TreeNode> CreateTree(PhysicalOperator *root,
	                                     size_t depth = 0);

	static size_t RenderTreeRecursive(TreeNode &node,
	                                  std::vector<std::string> &render,
	                                  std::vector<int> &render_heights,
	                                  size_t base_render_x = 0,
	                                  size_t start_depth = 0, int depth = 0);
	static std::string RenderTree(TreeNode &node);

  public:
	QueryProfiler() : enabled(false) {
	}

	void Enable() {
		enabled = true;
	}

	void Disable() {
		enabled = false;
	}

	void StartQuery(std::string query);
	void EndQuery();

	void StartOperator(PhysicalOperator *phys_op);
	void EndOperator(DataChunk &chunk);

	std::string ToString() const override;

  private:
	bool enabled;

	std::unique_ptr<TreeNode> root;
	std::string query;

	Profiler main_query;
	Profiler op;
	std::unordered_map<PhysicalOperator *, TreeNode *> tree_map;
	std::stack<PhysicalOperator *> execution_stack;
};
} // namespace duckdb
