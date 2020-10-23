#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.h"

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

TreeRenderer::TreeRenderer(unique_ptr<RenderTree> tree) :
	root(move(tree)) {

}

static void RenderTopLayer(RenderTree &root, std::stringstream &ss, idx_t y) {
	for(idx_t x = 0; x <= root.width; x++) {
		if (root.HasNode(x, y)) {
			ss << TreeRenderer::LTCORNER;
			ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2 - 1);
			if (y == 0) {
				// top level node: no node above this one
				ss << TreeRenderer::HORIZONTAL;
			} else {
				// render connection to node above this one
				ss << TreeRenderer::DMIDDLE;
			}
			ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2 - 1);
			ss << TreeRenderer::RTCORNER;
		} else {
			ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

static void RenderBottomLayer(RenderTree &root, std::stringstream &ss, idx_t y) {
	for(idx_t x = 0; x <= root.width; x++) {
		if (root.HasNode(x, y)) {
			ss << TreeRenderer::LDCORNER;
			ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2 - 1);
			if (root.HasNode(x, y + 1)) {
				// node below this one: connect to that one
				ss << TreeRenderer::TMIDDLE;
			} else {
				// no node below this one: end the box
				ss << TreeRenderer::HORIZONTAL;
			}
			ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2 - 1);
			ss << TreeRenderer::RDCORNER;
		} else if (root.HasNode(x, y + 1)) {
			ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH / 2);
			ss << TreeRenderer::VERTICAL;
			ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH / 2);
		} else {
			ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

string AdjustTextForRendering(string source, idx_t max_render_width) {
	idx_t cpos = 0;
	idx_t render_width = 0;
	vector<std::pair<idx_t, idx_t>> render_widths;
	while (cpos < source.size()) {
		idx_t char_render_width = utf8proc_render_width(source.c_str(), source.size(), cpos);
		cpos = utf8proc_next_grapheme_cluster(source.c_str(), source.size(), cpos);
		render_width += char_render_width;
		render_widths.push_back(std::make_pair(cpos, render_width));
		if (render_width > max_render_width) {
			break;
		}
	}
	if (render_width > max_render_width) {
		// need to find a position to truncate
		for(idx_t pos = render_widths.size(); pos > 0; pos--) {
			if (render_widths[pos - 1].second < max_render_width - 4) {
				return source.substr(0, render_widths[pos - 1].first) + "..." + string(max_render_width - render_widths[pos - 1].second - 3, ' ');
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

static void RenderBoxContent(RenderTree &root, std::stringstream &ss, idx_t y) {
	// we first need to figure out how high our boxes are going to be
	idx_t extra_height = 0;
	for(idx_t x = 0; x < root.width; x++) {
		auto node = root.GetNode(x, y);
		if (node) {
			if (node->extra_text.size() > extra_height) {
				extra_height = node->extra_text.size();
			}
		}
	}
	extra_height = MinValue<idx_t>(extra_height, TreeRenderer::MAX_EXTRA_LINES);
	idx_t halfway_point = (extra_height + 1) / 2;
	// now we render the actual node
	for(idx_t render_y = 0; render_y <= extra_height; render_y++) {
		for(idx_t x = 0; x < root.width; x++) {
			auto node = root.GetNode(x, y);
			if (!node) {
				if (render_y == halfway_point) {
					bool has_child_to_the_right = NodeHasMultipleChildren(root, x, y);
					if (root.HasNode(x, y + 1)) {
						// node right below this one
						ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2);
						ss << TreeRenderer::RTCORNER;
						if (has_child_to_the_right) {
							// but we have another child to the right! keep rendering the line
							ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH / 2);
						} else {
							// only a child below this one: fill the rest with spaces
							ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH / 2);
						}
					} else if (has_child_to_the_right) {
						// child to the right, but no child right below this one: render a full line
						ss << StringUtil::Repeat(TreeRenderer::HORIZONTAL, TreeRenderer::TREE_RENDER_WIDTH);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH);
					}
				} else if (render_y >= halfway_point) {
					if (root.HasNode(x, y + 1)) {
						// we have a node below this empty spot: render a vertical line
						ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH / 2);
						ss << TreeRenderer::VERTICAL;
						ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH / 2);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH);
					}
				} else {
					// empty spot: render spaces
					ss << StringUtil::Repeat(" ", TreeRenderer::TREE_RENDER_WIDTH);
				}
			} else {
				ss << TreeRenderer::VERTICAL;
				// figure out what to render
				string render_text;
				if (render_y == 0) {
					render_text = node->name;
				} else {
					if (render_y <= node->extra_text.size()) {
						render_text = node->extra_text[render_y - 1];
					}
				}
				render_text = AdjustTextForRendering(render_text, TreeRenderer::TREE_RENDER_WIDTH - 2);
				ss << render_text;

				if (render_y == halfway_point && NodeHasMultipleChildren(root, x, y)) {
					ss << TreeRenderer::LMIDDLE;
				} else {
					ss << TreeRenderer::VERTICAL;
				}
			}
		}
		ss << std::endl;
	}
}

string TreeRenderer::ToString() {
	std::stringstream ss;
	for(idx_t y = 0; y < root->height; y++) {
		// start by rendering the top layer
		RenderTopLayer(*root, ss, y);
		// now we render the content of the boxes
		RenderBoxContent(*root, ss, y);
		// render the bottom layer of each of the boxes
		RenderBottomLayer(*root, ss, y);
	}
	return ss.str();
}

static bool CanSplitOnThisChar(char l) {
	return l < '0' || (l > '9' && l < 'A') || (l > 'Z' && l < 'a');
}

static bool IsPadding(char l) {
	return l == ' ' || l == '\t';
}

static string RemovePadding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && IsPadding(l[start])) {
		start++;
	}
	while (end > 0 && IsPadding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

void SplitStringBuffer(const string &source, vector<string> &result) {
	constexpr idx_t MAX_LINE_RENDER_SIZE = TreeRenderer::TREE_RENDER_WIDTH - 2;
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
		size_t char_render_width = utf8proc_render_width(source.c_str(), source.size(), cpos);
		idx_t next_cpos = utf8proc_next_grapheme_cluster(source.c_str(), source.size(), cpos);
		if (render_width + char_render_width > MAX_LINE_RENDER_SIZE) {
			if (last_possible_split <= start_pos - 8) {
				last_possible_split = cpos;
			}
			result.push_back(source.substr(start_pos, last_possible_split - start_pos));
			start_pos = cpos;
			last_possible_split = cpos;
			render_width = 0;
		}
		cpos = next_cpos;
		render_width += char_render_width;
	}
	if (source.size() > start_pos) {
		result.push_back(source.substr(start_pos, source.size() - start_pos));
	}
}

template<class T>
unique_ptr<RenderTreeNode> CreateRenderNode(const T &op) {
	auto result = make_unique<RenderTreeNode>();
	result->name = op.GetName();
	auto extra_info = op.ParamsToString();
	if (extra_info.empty()) {
		return result;
	}
	result->extra_text.push_back(StringUtil::Repeat(string(TreeRenderer::HORIZONTAL) + " ", (TreeRenderer::TREE_RENDER_WIDTH - 7) / 2));
	auto splits = StringUtil::Split(extra_info, "\n");
	for(auto &split : splits) {
		string str = RemovePadding(split);
		SplitStringBuffer(str, result->extra_text);
	}
	return result;
}


unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const LogicalOperator &op) {
	return CreateRenderNode<LogicalOperator>(op);
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PhysicalOperator &op) {
	return CreateRenderNode<PhysicalOperator>(op);
}

template<class T>
static void GetTreeWidthHeight(const T &op, idx_t &width, idx_t &height) {
	if (op.children.size() == 0) {
		width = 1;
		height = 1;
		return;
	}
	width = 0;
	height = 0;

	for(auto &child : op.children) {
		idx_t child_width, child_height;
		GetTreeWidthHeight<T>(*child, child_width, child_height);
		width += child_width;
		height = MaxValue<idx_t>(height, child_height);
	}
	height++;
}

template<class T>
static idx_t CreateRenderTree(RenderTree &result, const T &op, idx_t x, idx_t y) {
	auto node = TreeRenderer::CreateNode(op);
	result.SetNode(x, y, move(node));

	if (op.children.size() == 0) {
		return 1;
	}
	idx_t width = 0;
	// render the children of this node
	for(auto &child : op.children) {
		width += CreateRenderTree<T>(result, *child, x + width, y + 1);
	}
	return width;
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const LogicalOperator &op) {
	idx_t width, height;
	GetTreeWidthHeight<LogicalOperator>(op, width, height);

	auto result = make_unique<RenderTree>(width, height);

	// now fill in the tree
	CreateRenderTree<LogicalOperator>(*result, op, 0, 0);
	return result;
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const PhysicalOperator &op) {
	idx_t width, height;
	GetTreeWidthHeight<PhysicalOperator>(op, width, height);

	auto result = make_unique<RenderTree>(width, height);

	// now fill in the tree
	CreateRenderTree<PhysicalOperator>(*result, op, 0, 0);
	return result;
}

}
