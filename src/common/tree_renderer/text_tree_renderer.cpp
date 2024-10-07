#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/typedefs.hpp"

#include <sstream>

namespace duckdb {

namespace {

struct StringSegment {
public:
	StringSegment(idx_t start, idx_t width) : start(start), width(width) {
	}

public:
	idx_t start;
	idx_t width;
};

} // namespace

void TextTreeRenderer::RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x < root.width; x++) {
		if (x * config.node_render_width >= config.maximum_render_width) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LTCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2 - 1);
			if (y == 0) {
				// top level node: no node above this one
				ss << config.HORIZONTAL;
			} else {
				// render connection to node above this one
				ss << config.DMIDDLE;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2 - 1);
			ss << config.RTCORNER;
		} else {
			bool has_adjacent_nodes = false;
			for (idx_t i = 0; x + i < root.width; i++) {
				has_adjacent_nodes = has_adjacent_nodes || root.HasNode(x + i, y);
			}
			if (!has_adjacent_nodes) {
				// There are no nodes to the right side of this position
				// no need to fill the empty space
				continue;
			}
			// there are nodes next to this, fill the space
			ss << StringUtil::Repeat(" ", config.node_render_width);
		}
	}
	ss << '\n';
}

static bool NodeHasMultipleChildren(RenderTreeNode &node) {
	return node.child_positions.size() > 1;
}

static bool ShouldRenderWhitespace(RenderTree &root, idx_t x, idx_t y) {
	idx_t found_children = 0;
	for (;; x--) {
		auto node = root.GetNode(x, y);
		if (root.HasNode(x, y + 1)) {
			found_children++;
		}
		if (node) {
			if (NodeHasMultipleChildren(*node)) {
				if (found_children < node->child_positions.size()) {
					return true;
				}
			}
			return false;
		}
		if (x == 0) {
			break;
		}
	}
	return false;
}

void TextTreeRenderer::RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x <= root.width; x++) {
		if (x * config.node_render_width >= config.maximum_render_width) {
			break;
		}
		bool has_adjacent_nodes = false;
		for (idx_t i = 0; x + i < root.width; i++) {
			has_adjacent_nodes = has_adjacent_nodes || root.HasNode(x + i, y);
		}
		auto node = root.GetNode(x, y);
		if (node) {
			ss << config.LDCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2 - 1);
			if (root.HasNode(x, y + 1)) {
				// node below this one: connect to that one
				ss << config.TMIDDLE;
			} else {
				// no node below this one: end the box
				ss << config.HORIZONTAL;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2 - 1);
			ss << config.RDCORNER;
		} else if (root.HasNode(x, y + 1)) {
			ss << StringUtil::Repeat(" ", config.node_render_width / 2);
			ss << config.VERTICAL;
			if (has_adjacent_nodes || ShouldRenderWhitespace(root, x, y)) {
				ss << StringUtil::Repeat(" ", config.node_render_width / 2);
			}
		} else {
			if (has_adjacent_nodes || ShouldRenderWhitespace(root, x, y)) {
				ss << StringUtil::Repeat(" ", config.node_render_width);
			}
		}
	}
	ss << '\n';
}

string AdjustTextForRendering(string source, idx_t max_render_width) {
	const idx_t size = source.size();
	const char *input = source.c_str();

	idx_t render_width = 0;

	// For every character in the input, create a StringSegment
	vector<StringSegment> render_widths;
	idx_t current_position = 0;
	while (current_position < size) {
		idx_t char_render_width = Utf8Proc::RenderWidth(input, size, current_position);
		current_position = Utf8Proc::NextGraphemeCluster(input, size, current_position);
		render_width += char_render_width;
		render_widths.push_back(StringSegment(current_position, render_width));
		if (render_width > max_render_width) {
			break;
		}
	}

	if (render_width > max_render_width) {
		// need to find a position to truncate
		for (idx_t pos = render_widths.size(); pos > 0; pos--) {
			auto &source_range = render_widths[pos - 1];
			if (source_range.width < max_render_width - 4) {
				return source.substr(0, source_range.start) + string("...") +
				       string(max_render_width - source_range.width - 3, ' ');
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

void TextTreeRenderer::RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y) {
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
	extra_height = MinValue<idx_t>(extra_height, config.max_extra_lines);
	idx_t halfway_point = (extra_height + 1) / 2;
	// now we render the actual node
	for (idx_t render_y = 0; render_y <= extra_height; render_y++) {
		for (idx_t x = 0; x < root.width; x++) {
			if (x * config.node_render_width >= config.maximum_render_width) {
				break;
			}
			bool has_adjacent_nodes = false;
			for (idx_t i = 0; x + i < root.width; i++) {
				has_adjacent_nodes = has_adjacent_nodes || root.HasNode(x + i, y);
			}
			auto node = root.GetNode(x, y);
			if (!node) {
				if (render_y == halfway_point) {
					bool has_child_to_the_right = ShouldRenderWhitespace(root, x, y);
					if (root.HasNode(x, y + 1)) {
						// node right below this one
						ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2);
						if (has_child_to_the_right) {
							ss << config.TMIDDLE;
							// but we have another child to the right! keep rendering the line
							ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width / 2);
						} else {
							ss << config.RTCORNER;
							if (has_adjacent_nodes) {
								// only a child below this one: fill the rest with spaces
								ss << StringUtil::Repeat(" ", config.node_render_width / 2);
							}
						}
					} else if (has_child_to_the_right) {
						// child to the right, but no child right below this one: render a full line
						ss << StringUtil::Repeat(config.HORIZONTAL, config.node_render_width);
					} else {
						if (has_adjacent_nodes) {
							// empty spot: render spaces
							ss << StringUtil::Repeat(" ", config.node_render_width);
						}
					}
				} else if (render_y >= halfway_point) {
					if (root.HasNode(x, y + 1)) {
						// we have a node below this empty spot: render a vertical line
						ss << StringUtil::Repeat(" ", config.node_render_width / 2);
						ss << config.VERTICAL;
						if (has_adjacent_nodes || ShouldRenderWhitespace(root, x, y)) {
							ss << StringUtil::Repeat(" ", config.node_render_width / 2);
						}
					} else {
						if (has_adjacent_nodes || ShouldRenderWhitespace(root, x, y)) {
							// empty spot: render spaces
							ss << StringUtil::Repeat(" ", config.node_render_width);
						}
					}
				} else {
					if (has_adjacent_nodes) {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.node_render_width);
					}
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
				if (render_y + 1 == extra_height && render_text.empty()) {
					auto entry = node->extra_text.find(RenderTreeNode::CARDINALITY);
					if (entry != node->extra_text.end()) {
						render_text = entry->second + " Rows";
					}
				}
				if (render_y == extra_height && render_text.empty()) {
					auto timing_entry = node->extra_text.find(RenderTreeNode::TIMING);
					if (timing_entry != node->extra_text.end()) {
						render_text = "(" + timing_entry->second + ")";
					} else if (node->extra_text.find(RenderTreeNode::CARDINALITY) == node->extra_text.end()) {
						// we only render estimated cardinality if there is no real cardinality
						auto entry = node->extra_text.find(RenderTreeNode::ESTIMATED_CARDINALITY);
						if (entry != node->extra_text.end()) {
							render_text = "~" + entry->second + " Rows";
						}
					}
					if (node->extra_text.find(RenderTreeNode::CARDINALITY) == node->extra_text.end()) {
						// we only render estimated cardinality if there is no real cardinality
						auto entry = node->extra_text.find(RenderTreeNode::ESTIMATED_CARDINALITY);
						if (entry != node->extra_text.end()) {
							render_text = "~" + entry->second + " Rows";
						}
					}
				}
				render_text = AdjustTextForRendering(render_text, config.node_render_width - 2);
				ss << render_text;

				if (render_y == halfway_point && NodeHasMultipleChildren(*node)) {
					ss << config.LMIDDLE;
				} else {
					ss << config.VERTICAL;
				}
			}
		}
		ss << '\n';
	}
}

string TextTreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TextTreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TextTreeRenderer::ToString(const ProfilingNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TextTreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void TextTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void TextTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void TextTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void TextTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void TextTreeRenderer::ToStreamInternal(RenderTree &root, std::ostream &ss) {
	while (root.width * config.node_render_width > config.maximum_render_width) {
		if (config.node_render_width - 2 < config.minimum_render_width) {
			break;
		}
		config.node_render_width -= 2;
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

bool TextTreeRenderer::CanSplitOnThisChar(char l) {
	return (l < '0' || (l > '9' && l < 'A') || (l > 'Z' && l < 'a')) && l != '_';
}

bool TextTreeRenderer::IsPadding(char l) {
	return l == ' ' || l == '\t' || l == '\n' || l == '\r';
}

string TextTreeRenderer::RemovePadding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && IsPadding(l[start])) {
		start++;
	}
	while (end > 0 && IsPadding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

void TextTreeRenderer::SplitStringBuffer(const string &source, vector<string> &result) {
	D_ASSERT(Utf8Proc::IsValid(source.c_str(), source.size()));
	const idx_t max_line_render_size = config.node_render_width - 2;
	// utf8 in prompt, get render width
	idx_t character_pos = 0;
	idx_t start_pos = 0;
	idx_t render_width = 0;
	idx_t last_possible_split = 0;

	const idx_t size = source.size();
	const char *input = source.c_str();

	while (character_pos < size) {
		size_t char_render_width = Utf8Proc::RenderWidth(input, size, character_pos);
		idx_t next_character_pos = Utf8Proc::NextGraphemeCluster(input, size, character_pos);

		// Does the next character make us exceed the line length?
		if (render_width + char_render_width > max_line_render_size) {
			if (start_pos + 8 > last_possible_split) {
				// The last character we can split on is one of the first 8 characters of the line
				// to not create very small lines we instead split on the current character
				last_possible_split = character_pos;
			}
			result.push_back(source.substr(start_pos, last_possible_split - start_pos));
			render_width = character_pos - last_possible_split;
			start_pos = last_possible_split;
			character_pos = last_possible_split;
		}
		// check if we can split on this character
		if (CanSplitOnThisChar(source[character_pos])) {
			last_possible_split = character_pos;
		}
		character_pos = next_character_pos;
		render_width += char_render_width;
	}
	if (size > start_pos) {
		// append the remainder of the input
		result.push_back(source.substr(start_pos, size - start_pos));
	}
}

void TextTreeRenderer::SplitUpExtraInfo(const InsertionOrderPreservingMap<string> &extra_info, vector<string> &result) {
	if (extra_info.empty()) {
		return;
	}
	for (auto &item : extra_info) {
		auto &text = item.second;
		if (!Utf8Proc::IsValid(text.c_str(), text.size())) {
			return;
		}
	}
	result.push_back(ExtraInfoSeparator());

	bool requires_padding = false;
	bool was_inlined = false;
	for (auto &item : extra_info) {
		string str = RemovePadding(item.second);
		if (str.empty()) {
			continue;
		}
		bool is_inlined = false;
		if (!StringUtil::StartsWith(item.first, "__")) {
			// the name is not internal (i.e. not __text__) - so we display the name in addition to the entry
			const idx_t available_width = (config.node_render_width - 7);
			idx_t total_size = item.first.size() + str.size() + 2;
			bool is_multiline = StringUtil::Contains(str, "\n");
			if (!is_multiline && total_size < available_width) {
				// we can inline the full entry - no need for any separators unless the previous entry explicitly
				// requires it
				str = item.first + ": " + str;
				is_inlined = true;
			} else {
				str = item.first + ":\n" + str;
			}
		}
		if (is_inlined && was_inlined) {
			// we can skip the padding if we have multiple inlined entries in a row
			requires_padding = false;
		}
		if (requires_padding) {
			result.emplace_back();
		}
		// cardinality, timing and estimated cardinality are rendered separately
		// this is to allow alignment horizontally across nodes
		if (item.first == RenderTreeNode::CARDINALITY) {
			// cardinality - need to reserve space for cardinality AND timing
			result.emplace_back();
			if (extra_info.find(RenderTreeNode::TIMING) != extra_info.end()) {
				result.emplace_back();
			}
			break;
		}
		if (item.first == RenderTreeNode::ESTIMATED_CARDINALITY) {
			// estimated cardinality - reserve space for estimate
			if (extra_info.find(RenderTreeNode::CARDINALITY) != extra_info.end()) {
				// if we have a true cardinality render that instead of the estimate
				result.pop_back();
				continue;
			}
			result.emplace_back();
			break;
		}
		auto splits = StringUtil::Split(str, "\n");
		for (auto &split : splits) {
			SplitStringBuffer(split, result);
		}
		requires_padding = true;
		was_inlined = is_inlined;
	}
}

string TextTreeRenderer::ExtraInfoSeparator() {
	return StringUtil::Repeat(string(config.HORIZONTAL), (config.node_render_width - 9));
}

} // namespace duckdb
