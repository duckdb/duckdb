//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mysql_tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/profiling_node.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

class MySQLTreeRenderer : public TreeRenderer {
public:
	explicit MySQLTreeRenderer() {
	}
	~MySQLTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const ProfilingNode &op, std::ostream &ss);
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStreamInternal(RenderTree &root, std::ostream &ss) override;

private:
	string LinePrefix(idx_t level);
	string FormatNodeName(string name);

	void RenderRecursive(idx_t level, RenderTree &tree, idx_t x, idx_t y, std::ostream &ss);
	string RenderTreeNodeToString(RenderTreeNode &node);
	string RenderDefaultTreeNodeToOneLineString(RenderTreeNode &node);
	string RenderProjectTreeNodeToOneLineString(RenderTreeNode &node);
	string RenderHashJoinTreeNodeToOneLineString(RenderTreeNode &node);
	string RenderScanTreeNodeToOneLineString(RenderTreeNode &node);
};

} // namespace duckdb
