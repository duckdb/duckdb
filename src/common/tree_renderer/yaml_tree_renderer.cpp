#include "duckdb/common/tree_renderer/yaml_tree_renderer.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "fmt/printf.h"

namespace duckdb {

string YAMLTreeRenderer::ToString(const LogicalOperator &op) {
	StringResultRenderer ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const PhysicalOperator &op) {
	StringResultRenderer ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const ProfilingNode &op) {
	StringResultRenderer ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const Pipeline &op) {
	StringResultRenderer ss;
	Render(op, ss);
	return ss.str();
}

void YAMLTreeRenderer::Render(const LogicalOperator &op, BaseResultRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const PhysicalOperator &op, BaseResultRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const ProfilingNode &op, BaseResultRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const Pipeline &op, BaseResultRenderer &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::ToStreamInternal(RenderTree &root, BaseResultRenderer &ss) {
	RenderRecursive(root, ss, 0, 0, 0);
}

static string EscapedString(const string &str) {
	string out = "\"";
	for (auto &ch : str) {
		switch (ch) {
		case '\b':
			out += "\\b";
			break;
		case '\f':
			out += "\\f";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		default:
			if ((unsigned char)ch < ' ') {
				out += duckdb_fmt::sprintf("\\u%04x", (int)ch);
			} else {
				out += ch;
			}
			break;
		}
	}
	out += "\"";
	return out;
}

void YAMLTreeRenderer::RenderRecursive(RenderTree &node, BaseResultRenderer &ss, idx_t indent, idx_t x, idx_t y) {
	auto node_p = node.GetNode(x, y);
	D_ASSERT(node_p);
	auto &current_node = *node_p;

	ss << string(indent, ' ') << "- name: " << EscapedString(current_node.name) << '\n';

	indent += 2; // rest attributes should align with leading "- "

	if (!current_node.extra_text.empty()) {
		for (const auto &info : current_node.extra_text) {
			auto &key = info.first;
			auto &value = info.second;
			auto splits = StringUtil::Split(value, "\n");
			ss << string(indent, ' ') << EscapedString(key) << ":";
			if (splits.size() > 1) { // list
				ss << '\n';
				for (const auto &split : splits) {
					ss << string(indent + 2, ' ') << "- " << EscapedString(split) << '\n';
				}
			} else {
				ss << ' ' << EscapedString(value) << '\n';
			}
		}
	}

	// Render children
	if (!current_node.child_positions.empty()) {
		ss << string(indent, ' ') << "children: \n";
		for (const auto &child_pos : current_node.child_positions) {
			RenderRecursive(node, ss, indent + 2, child_pos.x, child_pos.y);
		}
	}
}

} // namespace duckdb
