#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {

RenderTree::RenderTree(idx_t width_p, idx_t height_p) : width(width_p), height(height_p) {
	nodes = unique_ptr<unique_ptr<RenderTreeNode>[]>(new unique_ptr<RenderTreeNode>[(width + 1) * (height + 1)]);
}

RenderTreeNode *RenderTree::GetNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return nullptr;
	}
	return nodes[GetPosition(x, y)].get();
}

bool RenderTree::HasNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return false;
	}
	return nodes[GetPosition(x, y)].get() != nullptr;
}

idx_t RenderTree::GetPosition(idx_t x, idx_t y) {
	return y * width + x;
}

void RenderTree::SetNode(idx_t x, idx_t y, unique_ptr<RenderTreeNode> node) {
	nodes[GetPosition(x, y)] = move(node);
}

void TreeRenderer::RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x < root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LTCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (y == 0) {
				// top level node: no node above this one
				ss << config.HORIZONTAL;
			} else {
				// render connection to node above this one
				ss << config.DMIDDLE;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RTCORNER;
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

void TreeRenderer::RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x <= root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LDCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (root.HasNode(x, y + 1)) {
				// node below this one: connect to that one
				ss << config.TMIDDLE;
			} else {
				// no node below this one: end the box
				ss << config.HORIZONTAL;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RDCORNER;
		} else if (root.HasNode(x, y + 1)) {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
			ss << config.VERTICAL;
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

string AdjustTextForRendering(string source, idx_t max_render_width) {
	idx_t cpos = 0;
	idx_t render_width = 0;
	vector<pair<idx_t, idx_t>> render_widths;
	while (cpos < source.size()) {
		idx_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		render_width += char_render_width;
		render_widths.emplace_back(cpos, render_width);
		if (render_width > max_render_width) {
			break;
		}
	}
	if (render_width > max_render_width) {
		// need to find a position to truncate
		for (idx_t pos = render_widths.size(); pos > 0; pos--) {
			if (render_widths[pos - 1].second < max_render_width - 4) {
				return source.substr(0, render_widths[pos - 1].first) + "..." +
				       string(max_render_width - render_widths[pos - 1].second - 3, ' ');
			}
		}
		source = "...";
	}
	// need to pad with spaces
	idx_t total_spaces = max_render_width - render_width;
	idx_t half_spaces = total_spaces / 2;
	idx_t extra_left_space = total_spaces % 2 == 0 ? 0 : 1;
	return string(half_spaces + extra_left_space, ' ') + source + string(half_spaces, ' ');
}

static bool NodeHasMultipleChildren(RenderTree &root, idx_t x, idx_t y) {
	for (; x < root.width && !root.HasNode(x + 1, y); x++) {
		if (root.HasNode(x + 1, y + 1)) {
			return true;
		}
	}
	return false;
}

void TreeRenderer::RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y) {
	// we first need to figure out how high our boxes are going to be
	vector<vector<string>> extra_info;
	idx_t extra_height = 0;
	extra_info.resize(root.width);
	for (idx_t x = 0; x < root.width; x++) {
		auto node = root.GetNode(x, y);
		if (node) {
			SplitUpExtraInfo(node->extra_text, extra_info[x]);
			if (extra_info[x].size() > extra_height) {
				extra_height = extra_info[x].size();
			}
		}
	}
	extra_height = MinValue<idx_t>(extra_height, config.MAX_EXTRA_LINES);
	idx_t halfway_point = (extra_height + 1) / 2;
	// now we render the actual node
	for (idx_t render_y = 0; render_y <= extra_height; render_y++) {
		for (idx_t x = 0; x < root.width; x++) {
			if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
				break;
			}
			auto node = root.GetNode(x, y);
			if (!node) {
				if (render_y == halfway_point) {
					bool has_child_to_the_right = NodeHasMultipleChildren(root, x, y);
					if (root.HasNode(x, y + 1)) {
						// node right below this one
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						ss << config.RTCORNER;
						if (has_child_to_the_right) {
							// but we have another child to the right! keep rendering the line
							ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						} else {
							// only a child below this one: fill the rest with spaces
							ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						}
					} else if (has_child_to_the_right) {
						// child to the right, but no child right below this one: render a full line
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else if (render_y >= halfway_point) {
					if (root.HasNode(x, y + 1)) {
						// we have a node below this empty spot: render a vertical line
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						ss << config.VERTICAL;
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else {
					// empty spot: render spaces
					ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
				}
			} else {
				ss << config.VERTICAL;
				// figure out what to render
				string render_text;
				if (render_y == 0) {
					render_text = node->name;
				} else {
					if (render_y <= extra_info[x].size()) {
						render_text = extra_info[x][render_y - 1];
					}
				}
				render_text = AdjustTextForRendering(render_text, config.NODE_RENDER_WIDTH - 2);
				ss << render_text;

				if (render_y == halfway_point && NodeHasMultipleChildren(root, x, y)) {
					ss << config.LMIDDLE;
				} else {
					ss << config.VERTICAL;
				}
			}
		}
		ss << std::endl;
	}
}

string TreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const QueryProfiler::TreeNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void TreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const QueryProfiler::TreeNode &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	while (root.width * config.NODE_RENDER_WIDTH > config.MAXIMUM_RENDER_WIDTH) {
		if (config.NODE_RENDER_WIDTH - 2 < config.MINIMUM_RENDER_WIDTH) {
			break;
		}
		config.NODE_RENDER_WIDTH -= 2;
	}

	for (idx_t y = 0; y < root.height; y++) {
		// start by rendering the top layer
		RenderTopLayer(root, ss, y);
		// now we render the content of the boxes
		RenderBoxContent(root, ss, y);
		// render the bottom layer of each of the boxes
		RenderBottomLayer(root, ss, y);
	}
}

bool TreeRenderer::CanSplitOnThisChar(char l) {
	return (l < '0' || (l > '9' && l < 'A') || (l > 'Z' && l < 'a')) && l != '_';
}

bool TreeRenderer::IsPadding(char l) {
	return l == ' ' || l == '\t' || l == '\n' || l == '\r';
}

string TreeRenderer::RemovePadding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && IsPadding(l[start])) {
		start++;
	}
	while (end > 0 && IsPadding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

void TreeRenderer::SplitStringBuffer(const string &source, vector<string> &result) {
	D_ASSERT(Utf8Proc::IsValid(source.c_str(), source.size()));
	idx_t max_line_render_size = config.NODE_RENDER_WIDTH - 2;
	// utf8 in prompt, get render width
	idx_t cpos = 0;
	idx_t start_pos = 0;
	idx_t render_width = 0;
	idx_t last_possible_split = 0;
	while (cpos < source.size()) {
		// check if we can split on this character
		if (CanSplitOnThisChar(source[cpos])) {
			last_possible_split = cpos;
		}
		size_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		idx_t next_cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		if (render_width + char_render_width > max_line_render_size) {
			if (last_possible_split <= start_pos + 8) {
				last_possible_split = cpos;
			}
			result.push_back(source.substr(start_pos, last_possible_split - start_pos));
			start_pos = last_possible_split;
			cpos = last_possible_split;
			render_width = 0;
		}
		cpos = next_cpos;
		render_width += char_render_width;
	}
	if (source.size() > start_pos) {
		result.push_back(source.substr(start_pos, source.size() - start_pos));
	}
}

void TreeRenderer::SplitUpExtraInfo(const string &extra_info, vector<string> &result) {
	if (extra_info.empty()) {
		return;
	}
	if (!Utf8Proc::IsValid(extra_info.c_str(), extra_info.size())) {
		return;
	}
	auto splits = StringUtil::Split(extra_info, "\n");
	if (!splits.empty() && splits[0] != "[INFOSEPARATOR]") {
		result.push_back(ExtraInfoSeparator());
	}
	for (auto &split : splits) {
		if (split == "[INFOSEPARATOR]") {
			result.push_back(ExtraInfoSeparator());
			continue;
		}
		string str = RemovePadding(split);
		if (str.empty()) {
			continue;
		}
		SplitStringBuffer(str, result);
	}
}

string TreeRenderer::ExtraInfoSeparator() {
	return StringUtil::Repeat(string(config.HORIZONTAL) + " ", (config.NODE_RENDER_WIDTH - 7) / 2);
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateRenderNode(string name, string extra_info) {
	auto result = make_unique<RenderTreeNode>();
	result->name = move(name);
	result->extra_text = move(extra_info);
	return result;
}

class TreeChildrenIterator {
public:
	template <class T>
	static bool HasChildren(const T &op) {
		return !op.children.empty();
	}
	template <class T>
	static void Iterate(const T &op, const std::function<void(const T &child)> &callback) {
		for (auto &child : op.children) {
			callback(*child);
		}
	}
};

template <>
bool TreeChildrenIterator::HasChildren(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::DELIM_JOIN) {
		return true;
	}
	return !op.children.empty();
}
template <>
void TreeChildrenIterator::Iterate(const PhysicalOperator &op,
                                   const std::function<void(const PhysicalOperator &child)> &callback) {
	for (auto &child : op.children) {
		callback(*child);
	}
	if (op.type == PhysicalOperatorType::DELIM_JOIN) {
		auto &delim = (PhysicalDelimJoin &)op;
		callback(*delim.join);
	}
}

struct PipelineRenderNode {
	explicit PipelineRenderNode(PhysicalOperator &op) : op(op) {
	}

	PhysicalOperator &op;
	unique_ptr<PipelineRenderNode> child;
};

template <>
bool TreeChildrenIterator::HasChildren(const PipelineRenderNode &op) {
	return op.child.get();
}

template <>
void TreeChildrenIterator::Iterate(const PipelineRenderNode &op,
                                   const std::function<void(const PipelineRenderNode &child)> &callback) {
	if (op.child) {
		callback(*op.child);
	}
}

template <class T>
static void GetTreeWidthHeight(const T &op, idx_t &width, idx_t &height) {
	if (!TreeChildrenIterator::HasChildren(op)) {
		width = 1;
		height = 1;
		return;
	}
	width = 0;
	height = 0;

	TreeChildrenIterator::Iterate<T>(op, [&](const T &child) {
		idx_t child_width, child_height;
		GetTreeWidthHeight<T>(child, child_width, child_height);
		width += child_width;
		height = MaxValue<idx_t>(height, child_height);
	});
	height++;
}

template <class T>
idx_t TreeRenderer::CreateRenderTreeRecursive(RenderTree &result, const T &op, idx_t x, idx_t y) {
	auto node = TreeRenderer::CreateNode(op);
	result.SetNode(x, y, move(node));

	if (!TreeChildrenIterator::HasChildren(op)) {
		return 1;
	}
	idx_t width = 0;
	// render the children of this node
	TreeChildrenIterator::Iterate<T>(
	    op, [&](const T &child) { width += CreateRenderTreeRecursive<T>(result, child, x + width, y + 1); });
	return width;
}

template <class T>
unique_ptr<RenderTree> TreeRenderer::CreateRenderTree(const T &op) {
	idx_t width, height;
	GetTreeWidthHeight<T>(op, width, height);

	auto result = make_unique<RenderTree>(width, height);

	// now fill in the tree
	CreateRenderTreeRecursive<T>(*result, op, 0, 0);
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const LogicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PhysicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PipelineRenderNode &op) {
	return CreateNode(op.op);
}

string TreeRenderer::ExtractExpressionsRecursive(ExpressionInfo &state) {
	string result = "\n[INFOSEPARATOR]";
	result += "\n" + state.function_name;
	result += "\n" + StringUtil::Format("%.9f", double(state.function_time));
	if (state.children.empty()) {
		return result;
	}
	// render the children of this node
	for (auto &child : state.children) {
		result += ExtractExpressionsRecursive(*child);
	}
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const QueryProfiler::TreeNode &op) {
	auto result = TreeRenderer::CreateRenderNode(op.name, op.extra_info);
	result->extra_text += "\n[INFOSEPARATOR]";
	result->extra_text += "\n" + to_string(op.info.elements);
	string timing = StringUtil::Format("%.2f", op.info.time);
	result->extra_text += "\n(" + timing + "s)";
	if (config.detailed) {
		for (auto &info : op.info.executors_info) {
			if (!info) {
				continue;
			}
			for (auto &executor_info : info->roots) {
				string sample_count = to_string(executor_info->sample_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_count: " + sample_count;
				string sample_tuples_count = to_string(executor_info->sample_tuples_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_tuples_count: " + sample_tuples_count;
				string total_count = to_string(executor_info->total_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\ntotal_count: " + total_count;
				for (auto &state : executor_info->root->children) {
					result->extra_text += ExtractExpressionsRecursive(*state);
				}
			}
		}
	}
	return result;
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const LogicalOperator &op) {
	return CreateRenderTree<LogicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const PhysicalOperator &op) {
	return CreateRenderTree<PhysicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const QueryProfiler::TreeNode &op) {
	return CreateRenderTree<QueryProfiler::TreeNode>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const Pipeline &op) {
	auto operators = op.GetOperators();
	D_ASSERT(!operators.empty());
	unique_ptr<PipelineRenderNode> node;
	for (auto &op : operators) {
		auto new_node = make_unique<PipelineRenderNode>(*op);
		new_node->child = move(node);
		node = move(new_node);
	}
	return CreateRenderTree<PipelineRenderNode>(*node);
}

} // namespace duckdb