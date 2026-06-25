#pragma once

#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

class YAMLTreeRenderer : public TreeRenderer {
public:
	explicit YAMLTreeRenderer() {
	}
	~YAMLTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, BaseResultRenderer &ss);
	void Render(const PhysicalOperator &op, BaseResultRenderer &ss);
	void Render(const ProfilingNode &op, BaseResultRenderer &ss) override;
	void Render(const Pipeline &op, BaseResultRenderer &ss);

	void ToStreamInternal(RenderTree &root, BaseResultRenderer &ss) override;
	bool UsesRawKeyNames() override {
		return false;
	}

private:
	void RenderRecursive(RenderTree &node, BaseResultRenderer &ss, idx_t depth, idx_t x, idx_t y);
};

} // namespace duckdb
