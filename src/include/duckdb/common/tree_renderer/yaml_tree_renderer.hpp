#pragma once

#include "duckdb/main/profiling_node.hpp"
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

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const ProfilingNode &op, std::ostream &ss) override;
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStreamInternal(RenderTree &root, std::ostream &ss) override;
	bool UsesRawKeyNames() override {
		return false;
	}

private:
	void RenderRecursive(RenderTree &node, std::ostream &ss, idx_t depth, idx_t x, idx_t y);
};

} // namespace duckdb
