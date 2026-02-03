#include "duckdb/common/tree_renderer/mermaid_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {

string MermaidTreeRenderer::ToString(const LogicalOperator &op) {
	duckdb::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MermaidTreeRenderer::ToString(const PhysicalOperator &op) {
	duckdb::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MermaidTreeRenderer::ToString(const ProfilingNode &op) {
	duckdb::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string MermaidTreeRenderer::ToString(const Pipeline &op) {
	duckdb::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void MermaidTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MermaidTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MermaidTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void MermaidTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

static string SanitizeMermaidLabel(const string &text) {
	string result;
	result.reserve(text.size() * 2); // Reserve more space for potential escape sequences
	for (size_t i = 0; i < text.size(); i++) {
		char c = text[i];
		// Escape backticks and quotes
		if (c == '`') {
			result += "\\`";
		} else if (c == '"') {
			result += "\\\"";
		} else if (c == '\\' && i + 1 < text.size() && text[i + 1] == 'n') {
			// Replace literal "\n" with actual newline for Mermaid markdown
			result += "\n\t";
			i++; // Skip the 'n'
		} else {
			result += c;
		}
	}
	return result;
}

void MermaidTreeRenderer::ToStreamInternal(RenderTree &root, std::ostream &ss) {
	vector<string> nodes;
	vector<string> edges;

	const string node_format = "    node_%d_%d[\"`**%s**%s`\"]";

	for (idx_t y = 0; y < root.height; y++) {
		for (idx_t x = 0; x < root.width; x++) {
			auto node = root.GetNode(x, y);
			if (!node) {
				continue;
			}

			// Build node label with markdown formatting
			string extra_info;
			for (auto &item : node->extra_text) {
				auto &key = item.first;
				auto &value_raw = item.second;

				auto value = QueryProfiler::JSONSanitize(value_raw);
				// Add newline and key-value pair
				extra_info += StringUtil::Format("\n\t%s: %s", key, SanitizeMermaidLabel(value));
			}

			// Create node with bold operator name and extra info (trim name to remove trailing spaces)
			auto trimmed_name = node->name;
			StringUtil::Trim(trimmed_name);
			nodes.push_back(StringUtil::Format(node_format, x, y, SanitizeMermaidLabel(trimmed_name), extra_info));

			// Create Edge(s)
			for (auto &coord : node->child_positions) {
				edges.push_back(StringUtil::Format("    node_%d_%d --> node_%d_%d", x, y, coord.x, coord.y));
			}
		}
	}

	// Output Mermaid flowchart
	ss << "flowchart TD\n";

	// Output nodes
	for (auto &node : nodes) {
		ss << node << "\n\n";
	}

	// Output edges
	for (auto &edge : edges) {
		ss << edge << "\n";
	}
}

} // namespace duckdb
