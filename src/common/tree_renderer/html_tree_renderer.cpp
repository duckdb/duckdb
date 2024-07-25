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
	string style_section = R"(
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            position: relative;
        }

        .grid-container {
            display: grid;
            grid-template-columns: repeat(%d, minmax(150px, 1fr));
            grid-template-rows: repeat(%d, auto);
            gap: 20px;
            padding: 40px;
            position: relative;
        }

        .grid-item {
            border: 1px solid #000;
            padding: 10px;
            width: 150px;
            background-color: #f9f9f9;
            text-align: center;
            position: relative;
        }

        .title {
            font-weight: bold;
            border-bottom: 1px solid #000;
            margin-bottom: 5px;
            padding-bottom: 5px;
        }

        .content {
            text-align: center;
        }

        .sub-title {
            font-weight: bold;
            margin-top: 5px;
        }

        .value {
            margin-left: 10px;
        }

        .svg-container {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%%;
            height: 100%%;
            pointer-events: none;
            z-index: -1;
        }

        svg {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%%;
            height: 100%%;
        }

        .line {
            stroke: #000;
            stroke-width: 2;
            fill: none;
        }
    </style>
	)";
	auto columns = root.width;
	auto rows = root.height;
	return StringUtil::Format(style_section, columns, rows);
}

static string CreateHeadSection(RenderTree &root) {
	string head_section = R"(
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grid Layout</title>
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
        <div class="grid-item" id="item%d_%d" style="grid-column: %d / span 1; grid-row: %d / span 1;">
            <div class="title">%s</div>%s
        </div>
	)";

	auto node = root.GetNode(x, y);
	if (!node) {
		return "";
	}

	auto title = node->name;
	auto content = CreateGridItemContent(*node);
	return StringUtil::Format(grid_item_format, x, y, x + 1, y + 1, title, content);
}

static string CreateGridContainer(RenderTree &root) {
	vector<string> grid_items;
	for (idx_t y = 0; y < root.height; y++) {
		for (idx_t x = 0; x < root.width; x++) {
			auto node_item = CreateGridItem(root, x, y);
			if (!node_item.empty()) {
				grid_items.push_back(std::move(node_item));
			}
		}
	}

	const string grid_container_format = R"(
    <div class="grid-container">
	%s
    </div>
	)";

	return StringUtil::Format(grid_container_format, StringUtil::Join(grid_items, "\n"));
}

static string CreateSVGLines(RenderTree &root) {
	vector<string> function_body;
	vector<string> svg_lines;

	function_body.push_back("            const svg = document.getElementById('svgLines');");
	for (idx_t y = 0; y < root.height; y++) {
		for (idx_t x = 0; x < root.width; x++) {
			auto node = root.GetNode(x, y);
			if (!node) {
				continue;
			}
			function_body.push_back(StringUtil::Format(
			    "            const rect%d_%d = document.getElementById('item%d_%d').getBoundingClientRect();", x, y, x,
			    y));

			for (auto &coord : node->child_positions) {
				if (coord.x == x) {
					svg_lines.push_back(StringUtil::Format(
					    "            createVerticalConnection(svg, rect%d_%d, rect%d_%d);", x, y, coord.x, coord.y));
				} else {
					svg_lines.push_back(StringUtil::Format(
					    "            createAngledConnection(svg, rect%d_%d, rect%d_%d);", x, y, coord.x, coord.y));
				}
			}
		}
	}

	function_body.push_back("            svg.innerHTML = '';");
	function_body.push_back("            const svgRect = svg.getBoundingClientRect();");

	function_body.insert(function_body.end(), svg_lines.begin(), svg_lines.end());

	const string function_definition = R"(
        function updateSVGLines() {
%s
        }
	)";
	return StringUtil::Format(function_definition, StringUtil::Join(function_body, "\n"));
}

static string CreateBodySection(RenderTree &root) {
	const string body_section = R"(
<body>
%s

    <!-- SVG container for lines -->
    <div class="svg-container">
        <svg id="svgLines">
            <!-- Lines will be drawn here by JavaScript -->
        </svg>
    </div>

    <script>
        function createVerticalConnection(svg, item1, item2) {
            // Calculate positions for SVG
            const svgRect = svg.getBoundingClientRect();

            // Create the vertical line between item1 and item2
            const line3 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            const x = (item1.left + (item1.width / 2)) - svgRect.left;

            const y_start = item1.bottom - svgRect.top;
            const y_end = item2.top - svgRect.top;
            line3.setAttribute('x1', x);
            line3.setAttribute('y1', y_start);

            line3.setAttribute('x2', x);
            line3.setAttribute('y2', y_end);
            line3.setAttribute('class', 'line');
            svg.appendChild(line3);
        }

        function createAngledConnection(svg, item1, item2) {
            // Calculate positions for SVG
            const svgRect = svg.getBoundingClientRect();

            const x1 = item1.right - svgRect.left;
            const y1 = item1.top + (item1.height / 2) - svgRect.top;

            const x2 = (item2.left + (item2.width / 2)) - svgRect.left;
            const y2 = item2.top - svgRect.top;

            // Create the horizontal and vertical lines
            const line1 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line1.setAttribute('x1', x1);
            line1.setAttribute('y1', y1);
            line1.setAttribute('x2', x2);
            line1.setAttribute('y2', y1);
            line1.setAttribute('class', 'line');
            svg.appendChild(line1);

            const line2 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line2.setAttribute('x1', x2);
            line2.setAttribute('y1', y1);
            line2.setAttribute('x2', x2);
            line2.setAttribute('y2', y2);
            line2.setAttribute('class', 'line');
            svg.appendChild(line2);
        }

%s

        // Update SVG lines initially
        window.addEventListener('load', updateSVGLines);

        // Update SVG lines on window resize
        window.addEventListener('resize', updateSVGLines);
    </script>
</body>
</html>
	)";
	return StringUtil::Format(body_section, CreateGridContainer(root), CreateSVGLines(root));
}

void HTMLTreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	string result;
	result += CreateHeadSection(root);
	result += CreateBodySection(root);
	ss << result;
}

} // namespace duckdb
