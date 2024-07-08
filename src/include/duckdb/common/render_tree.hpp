//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/render_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/profiling_node.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

struct RenderTreeNode {
public:
	RenderTreeNode(const string &name, const string &extra_text) : name(name), extra_text(extra_text) {
	}

public:
	string name;
	string extra_text;
};

struct RenderTree {
	RenderTree(idx_t width, idx_t height);

	unique_array<unique_ptr<RenderTreeNode>> nodes;
	idx_t width;
	idx_t height;

public:
	static unique_ptr<RenderTree> CreateRenderTree(const LogicalOperator &op);
	static unique_ptr<RenderTree> CreateRenderTree(const PhysicalOperator &op);
	static unique_ptr<RenderTree> CreateRenderTree(const ProfilingNode &op);
	static unique_ptr<RenderTree> CreateRenderTree(const Pipeline &op);

public:
	optional_ptr<RenderTreeNode> GetNode(idx_t x, idx_t y);
	void SetNode(idx_t x, idx_t y, unique_ptr<RenderTreeNode> node);
	bool HasNode(idx_t x, idx_t y);

private:
	idx_t GetPosition(idx_t x, idx_t y);
};

} // namespace duckdb
