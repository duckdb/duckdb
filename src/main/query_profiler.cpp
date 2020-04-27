#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include <iostream>
#include <utility>

using namespace duckdb;
using namespace std;

constexpr idx_t TREE_RENDER_WIDTH = 20;
constexpr idx_t REMAINING_RENDER_WIDTH = TREE_RENDER_WIDTH - 2;
constexpr idx_t MAX_EXTRA_LINES = 10;

void QueryProfiler::StartQuery(string query, SQLStatement &statement) {
	if (!enabled) {
		return;
	}
	if (statement.type != StatementType::SELECT_STATEMENT && statement.type != StatementType::EXECUTE_STATEMENT) {
		return;
	}
	this->running = true;
	this->query = query;
	tree_map.clear();
	execution_stack = stack<PhysicalOperator *>();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();

	main_query.Start();
	op.Start();
}

void QueryProfiler::EndQuery() {
	if (!enabled || !running) {
		return;
	}

	main_query.End();
	this->running = false;
	// print the query after termination, if this is enabled
	if (automatic_print_format != ProfilerPrintFormat::NONE) {
		string query_info;
		if (automatic_print_format == ProfilerPrintFormat::JSON) {
			query_info = ToJSON();
		} else if (automatic_print_format == ProfilerPrintFormat::QUERY_TREE) {
			query_info = ToString();
		}

		if (save_location.empty()) {
			cout << query_info << "\n";
		} else {
			WriteToFile(save_location.c_str(), query_info);
		}
	}
}

void QueryProfiler::StartPhase(string new_phase) {
	if (!enabled || !running) {
		return;
	}

	if (!phase_stack.empty()) {
		// there are active phases
		phase_profiler.End();
		// add the timing to all phases prior to this one
		string prefix = "";
		for (auto &phase : phase_stack) {
			phase_timings[phase] += phase_profiler.Elapsed();
			prefix += phase + " > ";
		}
		// when there are previous phases, we prefix the current phase with those phases
		new_phase = prefix + new_phase;
	}

	// start a new phase
	phase_stack.push_back(new_phase);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
	if (!enabled || !running) {
		return;
	}
	assert(phase_stack.size() > 0);

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (phase_stack.size() > 0) {
		phase_profiler.Start();
	}
}

void QueryProfiler::StartOperator(PhysicalOperator *phys_op) {
	if (!enabled || !running) {
		return;
	}

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
	if (tree_map.count(phys_op) == 0) {
		// element does not exist in the tree! this only happens with a subquery
		// create a new tree
		assert(execution_stack.size() > 0);
		auto node = tree_map[execution_stack.top()];
		auto new_tree = CreateTree(phys_op, node->depth + 1);
		// add it to the current node
		node->children.push_back(move(new_tree));
	}
	execution_stack.push(phys_op);

	// start timing for current element
	op.Start();
}

void QueryProfiler::EndOperator(DataChunk &chunk) {
	if (!enabled || !running) {
		return;
	}

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
		       "Connection::EnableProfiling() to enable profiling!";
	}

	if (query.empty()) {
		return "<<Empty Profiling Information>>";
	}
	string result = "<<Query Profiling Information>>\n";
	result += StringUtil::Replace(query, "\n", " ") + "\n";
	result += "<<Timing>>\n";
	result += "Total Time: " + to_string(main_query.Elapsed()) + "s\n";
	// print phase timings
	for (const auto &entry : GetOrderedPhaseTimings()) {
		result += entry.first + ": " + to_string(entry.second) + "s\n";
	}
	// render the main operator tree
	result += "<<Operator Tree>>\n";
	if (!root) {
		result += "<<ERROR RENDERING ROOT>";
	} else {
		result += RenderTree(*root);
	}
	return result;
}

static string ToJSONRecursive(QueryProfiler::TreeNode &node) {
	string result = "{ \"name\": \"" + node.name + "\",\n";
	result += "\"timing\":" + StringUtil::Format("%.2f", node.info.time) + ",\n";
	result += "\"cardinality\":" + to_string(node.info.elements) + ",\n";
	result += "\"extra_info\": \"" + StringUtil::Replace(node.extra_info, "\n", "\\n") + "\",\n";
	result += "\"children\": [";
	result +=
	    StringUtil::Join(node.children, node.children.size(), ",\n",
	                     [](const unique_ptr<QueryProfiler::TreeNode> &child) { return ToJSONRecursive(*child); });
	result += "]\n}\n";
	return result;
}

string QueryProfiler::ToJSON() const {
	if (!enabled) {
		return "{ \"result\": \"disabled\" }\n";
	}
	if (query.empty()) {
		return "{ \"result\": \"empty\" }\n";
	}
	if (!root) {
		return "{ \"result\": \"error\" }\n";
	}
	string result = "{ \"result\": " + to_string(main_query.Elapsed()) + ",\n";
	// print the phase timings
	result += "\"timings\": {\n";
	const auto &ordered_phase_timings = GetOrderedPhaseTimings();
	result +=
	    StringUtil::Join(ordered_phase_timings, ordered_phase_timings.size(), ",\n", [](const PhaseTimingItem &entry) {
		    return "\"" + entry.first + "\": " + to_string(entry.second);
	    });
	result += "},\n";
	// recursively print the physical operator tree
	result += "\"tree\": ";
	result += ToJSONRecursive(*root);
	return result + "}";
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	ofstream out(path);
	out << info;
	out.close();
}

static bool is_non_split_char(char l) {
	return (l >= 65 && l <= 90) || (l >= 97 && l <= 122) || l == 95 || l == ']' || l == ')';
}

static bool is_padding(char l) {
	return l == ' ' || l == '(' || l == ')';
}

static string remove_padding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && is_padding(l[start])) {
		start++;
	}
	while (end > 0 && is_padding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

unique_ptr<QueryProfiler::TreeNode> QueryProfiler::CreateTree(PhysicalOperator *root, idx_t depth) {
	auto node = make_unique<QueryProfiler::TreeNode>();
	node->name = PhysicalOperatorToString(root->type);
	node->extra_info = root->ExtraRenderInformation();
	if (!node->extra_info.empty()) {
		auto splits = StringUtil::Split(node->extra_info, '\n');
		for (auto &split : splits) {
			string str = remove_padding(split);
			constexpr idx_t max_segment_size = REMAINING_RENDER_WIDTH - 2;
			idx_t location = 0;
			while (location < str.size() && node->split_extra_info.size() < MAX_EXTRA_LINES) {
				bool has_to_split = (str.size() - location) > max_segment_size;
				if (has_to_split) {
					// look for a split character
					idx_t i;
					for (i = 8; i < max_segment_size; i++) {
						if (!is_non_split_char(str[location + i])) {
							// split here
							break;
						}
					}
					node->split_extra_info.push_back(str.substr(location, i));
					location += i;
				} else {
					node->split_extra_info.push_back(str.substr(location));
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
	auto remaining_width = REMAINING_RENDER_WIDTH;
	if (text.size() > remaining_width) {
		text = text.substr(0, remaining_width);
	}
	assert(text.size() <= (idx_t)numeric_limits<int32_t>::max());

	auto right_padding = (remaining_width - text.size()) / 2;
	auto left_padding = remaining_width - text.size() - right_padding;
	return "|" + string(left_padding, padding_character) + text + string(right_padding, padding_character) + "|";
}

idx_t QueryProfiler::RenderTreeRecursive(QueryProfiler::TreeNode &node, vector<string> &render,
                                         vector<idx_t> &render_heights, idx_t base_render_x, idx_t start_depth,
                                         idx_t depth) {
	auto render_height = render_heights[depth];
	auto width = base_render_x;
	// render this node
	// first add any padding to render at this location
	auto start_position = width * TREE_RENDER_WIDTH;
	for (idx_t i = 0; i < render_height; i++) {
		if (render[start_depth + i].size() > start_position) {
			// something has already been rendered here!
			throw Exception("Tree rendering error, overlapping nodes!");
		} else {
			// add the padding
			render[start_depth + i] += string(start_position - render[start_depth + i].size(), ' ');
		}
	}

	// draw the boundaries of the box
	render[start_depth] += string(TREE_RENDER_WIDTH, '-');
	render[start_depth + render_height - 1] += string(TREE_RENDER_WIDTH, '-');

	// draw the name
	string name = node.name;
	render[start_depth + 1] += DrawPadded(name);
	// draw extra information
	for (idx_t i = 2; i < render_height - 3; i++) {
		auto split_index = i - 2;
		string string = split_index < node.split_extra_info.size() ? node.split_extra_info[split_index] : "";
		render[start_depth + i] += DrawPadded(string);
	}
	// draw the timing information
	string timing = StringUtil::Format("%.2f", node.info.time);
	render[start_depth + render_height - 3] += DrawPadded("(" + timing + "s)");
	// draw the intermediate count
	render[start_depth + render_height - 2] += DrawPadded(to_string(node.info.elements));

	for (auto &child : node.children) {
		// render all the children
		width =
		    RenderTreeRecursive(*child, render, render_heights, width, start_depth + render_heights[depth], depth + 1);
		width++;
	}
	if (node.children.size() > 0) {
		width--;
	}
	return width;
}

idx_t QueryProfiler::GetDepth(QueryProfiler::TreeNode &node) {
	idx_t depth = 0;
	for (auto &child : node.children) {
		depth = max(depth, GetDepth(*child));
	}
	return depth + 1;
}

static void GetRenderHeight(QueryProfiler::TreeNode &node, vector<idx_t> &render_heights, int depth = 0) {
	render_heights[depth] = max(render_heights[depth], (5 + (idx_t)node.split_extra_info.size()));
	for (auto &child : node.children) {
		GetRenderHeight(*child, render_heights, depth + 1);
	}
}

string QueryProfiler::RenderTree(QueryProfiler::TreeNode &node) {
	vector<idx_t> render_heights;
	// compute the height of each level
	auto depth = GetDepth(node);

	// compute the render height
	render_heights.resize(depth);
	GetRenderHeight(node, render_heights);
	int32_t total_height = 0;
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

void QueryProfiler::Print() {
	Printer::Print(ToString());
}

vector<QueryProfiler::PhaseTimingItem> QueryProfiler::GetOrderedPhaseTimings() const {
	vector<PhaseTimingItem> result;
	// first sort the phases alphabetically
	vector<string> phases;
	for (auto &entry : phase_timings) {
		phases.push_back(entry.first);
	}
	std::sort(phases.begin(), phases.end());
	for (const auto &phase : phases) {
		auto entry = phase_timings.find(phase);
		assert(entry != phase_timings.end());
		result.emplace_back(entry->first, entry->second);
	}
	return result;
}
