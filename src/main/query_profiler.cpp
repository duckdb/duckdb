
#include "main/query_profiler.hpp"

#include "execution/physical_operator.hpp"

using namespace duckdb;
using namespace std;

void QueryProfiler::StartQuery(std::string query) {
	if (!enabled)
		return;

	this->query = query;
	tree_map.clear();
	execution_stack = std::stack<PhysicalOperator *>();
	root = nullptr;

	main_query.Start();
}

void QueryProfiler::EndQuery() {
	if (!enabled)
		return;

	main_query.End();
}

void QueryProfiler::StartOperator(PhysicalOperator *phys_op) {
	if (!enabled)
		return;

	if (!root) {
		// start of execution: create operator tree
		root = CreateTree(phys_op);
	}
	if (!execution_stack.empty()) {
		// add timing for the previous element
		op.End();
		assert(tree_map.count(execution_stack.top()) > 0);
		auto &info = tree_map[execution_stack.top()]->info;
		info.time += op.Elapsed();
	}

	// push new element onto stack
	if (tree_map.count(phys_op) == 0) {
		auto node = tree_map[execution_stack.top()];
		// element does not exist in the tree! this only happens with a subquery
		// create a new tree
		auto new_tree = CreateTree(phys_op, node->depth + 1);
		// add it to the current node
		node->children.push_back(move(new_tree));
	}
	execution_stack.push(phys_op);

	// start timing for current element
	op.Start();
}

void QueryProfiler::EndOperator(DataChunk &chunk) {
	if (!enabled)
		return;

	// finish timing for the current element
	op.End();
	auto &info = tree_map[execution_stack.top()]->info;
	info.time += op.Elapsed();
	info.elements += chunk.size();

	assert(!execution_stack.empty());
	execution_stack.pop();

	// start timing again for the previous element, if any
	if (!execution_stack.empty()) {
		op.Start();
	}
}

std::string QueryProfiler::ToString() const {
	if (!enabled) {
		return "Query profiling is disabled. Call "
		       "DuckDBConnection::EnableProfiling() to enable profiling!";
	}

	if (query.empty()) {
		return "<<Empty Profiling Information>>";
	}
	std::string result = "<<Query Profiling Information>>\n";
	result += StringUtil::Replace(query, "\n", " ") + "\n";
	result += "<<Timing>>\n";
	result += "Total Time: " + std::to_string(main_query.Elapsed()) + "s\n";
	result += "<<Operator Tree>>\n";
	if (!root) {
		result += "<<ERROR RENDERING ROOT>";
	} else {
		result += RenderTree(*root);
	}
	return result;
}

std::unique_ptr<QueryProfiler::TreeNode>
QueryProfiler::CreateTree(PhysicalOperator *root, size_t depth) {
	auto node = make_unique<QueryProfiler::TreeNode>();
	node->name = PhysicalOperatorToString(root->type);
	node->depth = depth;
	tree_map[root] = node.get();
	for (auto &child : root->children) {
		auto child_node = CreateTree(child.get(), depth + 1);
		node->children.push_back(move(child_node));
	}
	return node;
}

constexpr size_t TREE_RENDER_HEIGHT = 5;
constexpr size_t TREE_RENDER_WIDTH = 20;

static std::string DrawPadded(std::string text, int remaining_width) {
	if (text.size() > (size_t)remaining_width) {
		text = text.substr(0, remaining_width);
	}
	int right_padding = (remaining_width - text.size()) / 2;
	int left_padding = remaining_width - text.size() - right_padding;
	return std::string(left_padding, ' ') + text +
	       std::string(right_padding, ' ') + "|";
}

size_t QueryProfiler::RenderTreeRecursive(QueryProfiler::TreeNode &node,
                                          std::vector<std::string> &render,
                                          size_t base_render_x) {
	size_t width = base_render_x;
	// render this node
	// first add any padding to render at this location
	size_t start_depth = node.depth * TREE_RENDER_HEIGHT;
	size_t start_position = width * TREE_RENDER_WIDTH;
	for (size_t i = 0; i < TREE_RENDER_HEIGHT; i++) {
		if (render[start_depth + i].size() > start_position) {
			// something has already been rendered here!
			throw Exception("Tree rendering error, overlapping nodes!");
		} else {
			// add the padding
			render[start_depth + i] += std::string(
			    start_position - render[start_depth + i].size(), ' ');
		}
	}
	// draw the boundaries of the box
	render[start_depth] += std::string(TREE_RENDER_WIDTH, '-');
	render[start_depth + TREE_RENDER_HEIGHT - 1] +=
	    std::string(TREE_RENDER_WIDTH, '-');
	for (size_t i = 1; i < TREE_RENDER_HEIGHT - 1; i++) {
		render[start_depth + i] += "|";
	}

	int remaining_width = TREE_RENDER_WIDTH - 2;

	// draw the name
	std::string name = node.name;
	render[start_depth + 1] += DrawPadded(name, remaining_width);
	// draw the timing information
	std::string timing = StringUtil::Format("%.2f", node.info.time);
	render[start_depth + 2] += DrawPadded(timing + "s", remaining_width);
	// draw the intermediate count
	render[start_depth + 3] +=
	    DrawPadded(std::to_string(node.info.elements), remaining_width);

	for (auto &child : node.children) {
		// render all the children
		width = RenderTreeRecursive(*child, render, width);
		width++;
	}
	if (node.children.size() > 0) {
		width--;
	}
	return width;
}

size_t QueryProfiler::GetDepth(QueryProfiler::TreeNode &node) {
	size_t depth = 0;
	for (auto &child : node.children) {
		depth = std::max(depth, GetDepth(*child));
	}
	return depth + 1;
}

std::string QueryProfiler::RenderTree(QueryProfiler::TreeNode &node) {
	std::vector<std::string> render;
	render.resize(GetDepth(node) * TREE_RENDER_HEIGHT);
	RenderTreeRecursive(node, render, 0);
	std::string text;
	for (auto &str : render) {
		text += str + "\n";
	}
	return text;
}
