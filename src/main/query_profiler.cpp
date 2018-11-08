
#include "main/query_profiler.hpp"

#include "execution/physical_operator.hpp"

using namespace duckdb;
using namespace std;

constexpr size_t TREE_RENDER_WIDTH = 20;
constexpr size_t REMAINING_RENDER_WIDTH = TREE_RENDER_WIDTH - 2;
constexpr size_t MAX_EXTRA_LINES = 10;

void QueryProfiler::StartQuery(string query) {
	if (!enabled)
		return;

	this->query = query;
	tree_map.clear();
	execution_stack = stack<PhysicalOperator *>();
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

string QueryProfiler::ToString() const {
	if (!enabled) {
		return "Query profiling is disabled. Call "
		       "DuckDBConnection::EnableProfiling() to enable profiling!";
	}

	if (query.empty()) {
		return "<<Empty Profiling Information>>";
	}
	string result = "<<Query Profiling Information>>\n";
	result += StringUtil::Replace(query, "\n", " ") + "\n";
	result += "<<Timing>>\n";
	result += "Total Time: " + to_string(main_query.Elapsed()) + "s\n";
	result += "<<Operator Tree>>\n";
	if (!root) {
		result += "<<ERROR RENDERING ROOT>";
	} else {
		result += RenderTree(*root);
	}
	return result;
}

static bool is_non_split_char(char l) {
	return (l >= 65 && l <= 90) || (l >= 97 && l <= 122) || l == 95 ||
	       l == ']' || l == ')';
}

static bool is_padding(char l) {
	return l == ' ' || l == '(' || l == ')';
}

static string remove_padding(string l) {
	size_t start = 0, end = l.size();
	while (start < l.size() && is_padding(l[start])) {
		start++;
	}
	while (end > 0 && is_padding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

unique_ptr<QueryProfiler::TreeNode>
QueryProfiler::CreateTree(PhysicalOperator *root, size_t depth) {
	auto node = make_unique<QueryProfiler::TreeNode>();
	node->name = PhysicalOperatorToString(root->type);
	auto extra_info = root->ExtraRenderInformation();
	if (!extra_info.empty()) {
		auto splits = StringUtil::Split(extra_info, '\n');
		for (auto &split : splits) {
			string str = remove_padding(split);
			constexpr size_t max_segment_size = REMAINING_RENDER_WIDTH - 2;
			size_t location = 0;
			while (location < str.size() &&
			       node->extra_info.size() < MAX_EXTRA_LINES) {
				bool has_to_split = (str.size() - location) > max_segment_size;
				if (has_to_split) {
					// look for a split character
					size_t i;
					for (i = 8; i < max_segment_size; i++) {
						if (!is_non_split_char(str[location + i])) {
							// split here
							break;
						}
					}
					node->extra_info.push_back(str.substr(location, i));
					location += i;
				} else {
					node->extra_info.push_back(str.substr(location));
					break;
				}
			}
		}
	}
	node->depth = depth;
	tree_map[root] = node.get();
	for (auto &child : root->children) {
		auto child_node = CreateTree(child.get(), depth + 1);
		node->children.push_back(move(child_node));
	}
	return node;
}

static string DrawPadded(string text, char padding_character = ' ') {
	int remaining_width = REMAINING_RENDER_WIDTH;
	if (text.size() > (size_t)remaining_width) {
		text = text.substr(0, remaining_width);
	}
	int right_padding = (remaining_width - text.size()) / 2;
	int left_padding = remaining_width - text.size() - right_padding;
	return "|" + string(left_padding, padding_character) + text +
	       string(right_padding, padding_character) + "|";
}

size_t QueryProfiler::RenderTreeRecursive(QueryProfiler::TreeNode &node,
                                          vector<string> &render,
                                          vector<int> &render_heights,
                                          size_t base_render_x,
                                          size_t start_depth, int depth) {
	int render_height = render_heights[depth];
	size_t width = base_render_x;
	// render this node
	// first add any padding to render at this location
	size_t start_position = width * TREE_RENDER_WIDTH;
	for (size_t i = 0; i < render_height; i++) {
		if (render[start_depth + i].size() > start_position) {
			// something has already been rendered here!
			throw Exception("Tree rendering error, overlapping nodes!");
		} else {
			// add the padding
			render[start_depth + i] +=
			    string(start_position - render[start_depth + i].size(), ' ');
		}
	}

	// draw the boundaries of the box
	render[start_depth] += string(TREE_RENDER_WIDTH, '-');
	render[start_depth + render_height - 1] += string(TREE_RENDER_WIDTH, '-');

	// draw the name
	string name = node.name;
	render[start_depth + 1] += DrawPadded(name);
	// draw extra information
	for (size_t i = 2; i < render_height - 3; i++) {
		size_t split_index = i - 2;
		string string = split_index < node.extra_info.size()
		                    ? node.extra_info[split_index]
		                    : "";
		render[start_depth + i] += DrawPadded(string);
	}
	// draw the timing information
	string timing = StringUtil::Format("%.2f", node.info.time);
	render[start_depth + render_height - 3] += DrawPadded("(" + timing + "s)");
	// draw the intermediate count
	render[start_depth + render_height - 2] +=
	    DrawPadded(to_string(node.info.elements));

	for (auto &child : node.children) {
		// render all the children
		width =
		    RenderTreeRecursive(*child, render, render_heights, width,
		                        start_depth + render_heights[depth], depth + 1);
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
		depth = max(depth, GetDepth(*child));
	}
	return depth + 1;
}

static void GetRenderHeight(QueryProfiler::TreeNode &node,
                            vector<int> &render_heights, int depth = 0) {
	render_heights[depth] =
	    max((int)render_heights[depth], (int)(5 + node.extra_info.size()));
	for (auto &child : node.children) {
		GetRenderHeight(*child, render_heights, depth + 1);
	}
}

string QueryProfiler::RenderTree(QueryProfiler::TreeNode &node) {
	vector<int> render_heights;
	// compute the height of each level
	int depth = GetDepth(node);

	// compute the render height
	render_heights.resize(depth);
	GetRenderHeight(node, render_heights);
	int total_height = 0;
	for (auto height : render_heights) {
		total_height += height;
	}

	// now actually render the tree
	vector<string> render;
	render.resize(total_height);

	RenderTreeRecursive(node, render, render_heights);
	string text;
	for (auto &str : render) {
		text += str + "\n";
	}
	return text;
}
