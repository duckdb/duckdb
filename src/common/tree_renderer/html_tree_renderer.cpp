#include "duckdb/common/tree_renderer/html_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {

string HTMLTreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const ProfilingNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string HTMLTreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void HTMLTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void HTMLTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

static string CreateStyleSection(RenderTree &root) {
	return R"(
    <style>
        body {
            font-family: Arial, sans-serif;
        }

		.tf-tree .tf-nc {
			padding: 0px;
            border: 1px solid #E5E5E5;
		}

        .tf-nc {
            border-radius: 0.5rem;
            padding: 0px;
			min-width: 150px;
            width: auto;
            background-color: #FAFAFA;
            text-align: center;
            position: relative;
        }

        .title {
            font-weight: bold;
            padding-bottom: 5px;
            color: #fff100;
            box-sizing: border-box;
            background-color: black;
            border-top-left-radius: 0.5rem;
            border-top-right-radius: 0.5rem;
            padding: 10px;
        }

        .tf-nc > .title:only-child {
            border-bottom-left-radius: 0.5rem;
            border-bottom-right-radius: 0.5rem;
        }

        .content {
            border-top: 1px solid #000;
            text-align: center;
            border-bottom-left-radius: 0.5rem;
            border-bottom-right-radius: 0.5rem;
            padding: 10px;
        }

        .sub-title {
            color: black;
            font-weight: bold;
            padding-top: 5px;
        }

        .sub-title:not(:first-child) {
            border-top: 1px solid #ADADAD;
        }

        .value {
            margin-left: 10px;
            margin-top: 5px;
            color: #3B3B3B;
            margin-bottom: 5px;
        }

        .tf-tree {
            display: grid;
            box-sizing: border-box;
            justify-content: start;
            align-items: start;
            width: 100%;
            height: 100%;
            overflow: auto;
        }
    </style>
	)";
}

static string CreateHeadSection(RenderTree &root) {
	string head_section = R"(
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
	<link rel="stylesheet" href="https://unpkg.com/treeflex/dist/css/treeflex.css">
    <title>DuckDB Query Plan</title>
	%s
</head>
	)";
	return StringUtil::Format(head_section, CreateStyleSection(root));
}

static string CreateGridItemContent(RenderTreeNode &node) {
	const string content_format = R"(
            <div class="content">
%s
            </div>
	)";

	if (node.extra_text.empty()) {
		return "";
	}

	vector<string> items;
	for (auto &item : node.extra_text) {
		auto &key = item.first;
		auto &value = item.second;

		if (value.empty()) {
			continue;
		}

		items.push_back(StringUtil::Format(R"(                <div class="sub-title">%s</div>)", key));
		auto splits = StringUtil::Split(value, "\n");
		for (auto &split : splits) {
			items.push_back(StringUtil::Format(R"(                <div class="value">%s</div>)", split));
		}
	}
	return StringUtil::Format(content_format, StringUtil::Join(items, "\n"));
}

static string CreateGridItem(RenderTree &root, idx_t x, idx_t y) {
	const string grid_item_format = R"(
        <div class="tf-nc">
            <div class="title">%s</div>%s
        </div>
	)";

	auto node = root.GetNode(x, y);
	if (!node) {
		return "";
	}

	auto title = node->name;
	auto content = CreateGridItemContent(*node);
	return StringUtil::Format(grid_item_format, title, content);
}

static string CreateTreeRecursive(RenderTree &root, idx_t x, idx_t y) {
	string result;

	result += "<li>";
	result += CreateGridItem(root, x, y);
	auto node = root.GetNode(x, y);
	if (!node->child_positions.empty()) {
		result += "<ul>";
		for (auto &coord : node->child_positions) {
			result += CreateTreeRecursive(root, coord.x, coord.y);
		}
		result += "</ul>";
	}
	result += "</li>";
	return result;
}

static string CreateBodySection(RenderTree &root) {
	const string body_section = R"(
<body>
	<div class="tf-tree">
		<ul>%s</ul>
	</div>
</body>
</html>
	)";
	return StringUtil::Format(body_section, CreateTreeRecursive(root, 0, 0));
}

void HTMLTreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	string result;
	result += CreateHeadSection(root);
	result += CreateBodySection(root);
	ss << result;
}

} // namespace duckdb
