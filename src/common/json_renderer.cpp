#include "duckdb/common/json_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utf8proc_wrapper.hpp"

#include "yyjson.hpp"

#include <sstream>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

string JSONRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONRenderer::ToString(const QueryProfiler::TreeNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void JSONRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONRenderer::Render(const QueryProfiler::TreeNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

static void RenderRecursive(RenderTree &tree, RenderTreeNode::Coordinate &pos, std::ostream &ss, idx_t depth) {
	auto indent = string(depth * 2, ' ');
	auto node_p = tree.GetNode(pos.x, pos.y);
	D_ASSERT(node_p);
	auto &node = *node_p;
	ss << StringUtil::Format("%s\"name\": \"%s\",\n", indent, node.name);
	ss << StringUtil::Format("%s\"children\": [\n", indent);
	for (auto &child_pos : node.child_positions) {
		ss << StringUtil::Format("%s  {\n", indent);
		RenderRecursive(tree, child_pos, ss, depth + 2);
		ss << StringUtil::Format("%s  }\n", indent);
	}
	ss << StringUtil::Format("%s]\n", indent);
}

void JSONRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	ss << "[\n";
	ss << "  {\n";
	RenderTreeNode::Coordinate pos(0, 0);
	RenderRecursive(root, pos, ss, 2);
	ss << "  }\n";
	ss << "]\n";
}

} // namespace duckdb
