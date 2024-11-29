#include "duckdb/common/tree_renderer/mysql_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {
string MySQLTreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MySQLTreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MySQLTreeRenderer::ToString(const ProfilingNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MySQLTreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void MySQLTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MySQLTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MySQLTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MySQLTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

string MySQLTreeRenderer::FormatNodeName(string name) {
	string suffix;
	if (StringUtil::Contains(name, "AGGREGATE") || StringUtil::Equals(name.c_str(), "FILTER") ||
	    StringUtil::Equals(name.c_str(), "PROJECTION") || StringUtil::Equals(name.c_str(), "ORDER_BY")) {
		suffix = ":";
	}
	string format_text = StringUtil::Replace(name, "_", " ");
	StringUtil::RTrim(format_text);
	return StringUtil::Title(format_text) + suffix;
}

string MySQLTreeRenderer::RenderDefaultTreeNodeToOneLineString(RenderTreeNode &node) {
	string text = FormatNodeName(node.name);
	string estimated_profile, actual_profile;
	for (auto &it : node.extra_text) {
		auto &key = it.first;
		auto &value = it.second;
		if (key == "Estimated Cardinality") {
			estimated_profile = "(rows=" + value + ")";
		} else if (key == "Cardinality") {
			actual_profile = "(actual rows=" + value;
		} else if (key == "Timing") {
			actual_profile += " actual time=" + value + ")";
		} else if (value.length() > 0) {
			auto values = StringUtil::Replace(value, "\n", " ");
			if (node.name == "UNGROUPED_AGGREGATE" && key == "Aggregates") {
				text += " " + values;
			} else {
				text += " " + key + ": " + values;
			}
		}
	}

	StringUtil::RTrim(text, ":");
	if (estimated_profile.length())
		text += " " + estimated_profile;
	if (actual_profile.length())
		text += " " + actual_profile;
	return text;
}

string MySQLTreeRenderer::RenderTreeNodeToString(RenderTreeNode &node) {
	string render_text;
	if (node.name == "PROJECTION" || node.name == "FILTER" || node.name == "ORDER_BY") {
		render_text = RenderProjectTreeNodeToOneLineString(node);
	} else if (node.name == "HASH_JOIN" || node.name == "RIGHT_DELIM_JOIN") {
		render_text = RenderProjectTreeNodeToOneLineString(node);
	} else {
		render_text = RenderDefaultTreeNodeToOneLineString(node);
	}
	return render_text;
}

void MySQLTreeRenderer::RenderRecursive(idx_t level, RenderTree &tree, idx_t x, idx_t y, std::ostream &ss) {
	auto node_p = tree.GetNode(x, y);
	D_ASSERT(node_p);
	auto &node = *node_p;

	auto render_text = LinePrefix(level) + RenderTreeNodeToString(node);
	ss << render_text;
	ss << "\n";

	for (auto &child_pos : node.child_positions) {
		RenderRecursive(level + 1, tree, child_pos.x, child_pos.y, ss);
	}
}

void MySQLTreeRenderer::ToStreamInternal(RenderTree &root, std::ostream &ss) {
	RenderRecursive(0, root, 0, 0, ss);
}

string MySQLTreeRenderer::LinePrefix(idx_t level) {
	// return StringUtil::Repeat(" ", (level * 2)) + " -> COLUMNSTORE ";
	return StringUtil::Repeat(" ", (level * 2)) + " -> ";
}

string MySQLTreeRenderer::RenderProjectTreeNodeToOneLineString(RenderTreeNode &node) {
	string text = FormatNodeName(node.name);
	string estimated_profile, actual_profile;
	for (auto &it : node.extra_text) {
		auto &key = it.first;
		auto &value = it.second;
		if (key == "Estimated Cardinality") {
			estimated_profile = "(rows=" + value + ")";
		} else if (key == "Cardinality") {
			actual_profile = "(actual rows=" + value;
		} else if (key == "Timing") {
			actual_profile += " actual time=" + value + ")";
		} else {
			auto values = StringUtil::Replace(value, "\n", " ");
			if (key == "Projections" || key == "Expression" || key == "Expressions" || key == "Order By") {
				// omit duplicated words.
				text += " " + values;
			} else if (key == "Conditions") {
				text += " (" + values + ")";
			} else if (key == "Join Type") {
				auto values_title = StringUtil::Title(values);
				if (!StringUtil::Contains(text, values_title)) {
					text = values_title + " " + text;
				}
			} else {
				text += " " + key + ": " + values;
			}
		}
	}
	if (estimated_profile.length())
		if (text.back() != ' ')
			text += " ";
	text += estimated_profile;
	if (actual_profile.length())
		if (text.back() != ' ')
			text += " ";
	text += actual_profile;
	return text;
}

string MySQLTreeRenderer::RenderHashJoinTreeNodeToOneLineString(RenderTreeNode &node) {
	return string();
}

string MySQLTreeRenderer::RenderScanTreeNodeToOneLineString(RenderTreeNode &node) {
	return string();
}

} // namespace duckdb
