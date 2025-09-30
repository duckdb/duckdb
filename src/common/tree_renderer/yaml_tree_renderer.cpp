#include "duckdb/common/tree_renderer/yaml_tree_renderer.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "fmt/printf.h"

#include <ostream>
#include <sstream>

namespace duckdb {

string YAMLTreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const ProfilingNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string YAMLTreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void YAMLTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void YAMLTreeRenderer::ToStreamInternal(RenderTree &root, std::ostream &ss) {
	RenderRecursive(root, ss, 0, 0, 0);
}

struct EscapedString {
	explicit EscapedString(const string &str) : str(str) {};
	const string &str;
};

std::ostream &operator<<(std::ostream &out, const EscapedString &es) {
	out << '"';

	// escape
	for (auto &ch : es.str) {
		switch (ch) {
		case '\b':
			out << "\\b";
			break;
		case '\f':
			out << "\\f";
			break;
		case '\n':
			out << "\\n";
			break;
		case '\r':
			out << "\\r";
			break;
		case '\t':
			out << "\\t";
			break;
		case '"':
			out << "\\\"";
			break;
		case '\\':
			out << "\\\\";
			break;
		default:
			if ((unsigned char)ch < ' ') {
				out << duckdb_fmt::sprintf("\\u%04x", (int)ch);
			} else {
				out << ch;
			}
			break;
		}
	}

	out << '"';
	return out;
}

void YAMLTreeRenderer::RenderRecursive(RenderTree &node, std::ostream &ss, idx_t indent, idx_t x, idx_t y) {
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
