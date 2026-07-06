#include "shell_manual.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

#include <functional>

namespace duckdb_shell {

using duckdb::StringUtil;
using duckdb::Utf8Proc;

namespace {

//===--------------------------------------------------------------------===//
// Render-width + word-wrap helpers (UTF8-aware, ported from text_tree_renderer)
//===--------------------------------------------------------------------===//
//! Visible display width, skipping ANSI escape sequences (ESC [ ... m) so that syntax-highlighted
//! text measures by its rendered width rather than its byte content.
idx_t RenderLength(const string &str) {
	if (str.find('\033') == string::npos) {
		return Utf8Proc::RenderWidth(str);
	}
	string visible;
	visible.reserve(str.size());
	for (idx_t i = 0; i < str.size();) {
		if (str[i] == '\033' && i + 1 < str.size() && str[i + 1] == '[') {
			i += 2;
			while (i < str.size() && str[i] != 'm') {
				i++;
			}
			if (i < str.size()) {
				i++; // skip the terminating 'm'
			}
			continue;
		}
		visible += str[i++];
	}
	return Utf8Proc::RenderWidth(visible);
}

//! Whether the byte starts a codepoint (i.e. is not a UTF8 continuation byte)
bool IsCharacter(char c) {
	return (static_cast<uint8_t>(c) & 0xC0) != 0x80;
}

string TruncateText(const string &text, idx_t max_width) {
	if (RenderLength(text) <= max_width) {
		return text;
	}
	if (max_width <= 1) {
		return "\xE2\x80\xA6"; // "…"
	}
	string result;
	idx_t width = 0;
	for (idx_t i = 0; i < text.size();) {
		idx_t next = i + 1;
		while (next < text.size() && !IsCharacter(text[next])) {
			next++;
		}
		auto char_width = RenderLength(text.substr(i, next - i));
		if (width + char_width > max_width - 1) {
			break;
		}
		result += text.substr(i, next - i);
		width += char_width;
		i = next;
	}
	return result + "\xE2\x80\xA6";
}

//! Word-wrap a single string across lines of `budget` display columns, breaking at spaces.
vector<string> WordWrap(const string &text, idx_t budget) {
	if (budget < 2) {
		budget = 2;
	}
	vector<string> lines;
	string line;
	idx_t line_width = 0;
	for (idx_t i = 0; i < text.size();) {
		idx_t j = i;
		while (j < text.size() && text[j] != ' ') {
			j++;
		}
		string word = text.substr(i, j - i);
		i = j < text.size() ? j + 1 : j;
		if (word.empty()) {
			continue;
		}
		if (RenderLength(word) > budget) {
			word = TruncateText(word, budget);
		}
		idx_t word_width = RenderLength(word);
		idx_t needed = word_width + (line.empty() ? 0 : 1);
		if (line_width + needed <= budget) {
			if (!line.empty()) {
				line += ' ';
				line_width++;
			}
			line += word;
			line_width += word_width;
		} else {
			if (!line.empty()) {
				lines.push_back(std::move(line));
			}
			line = std::move(word);
			line_width = word_width;
		}
	}
	if (!line.empty() || lines.empty()) {
		lines.push_back(std::move(line));
	}
	return lines;
}

//! Applies syntax highlighting to a plain-text fragment, returning it with embedded ANSI codes.
//! Returns the input unchanged when highlighting is disabled.
using Highlighter = std::function<string(const string &)>;

// Each indentation level is two spaces; content sits at one level.
constexpr idx_t INDENT = 2;

//===--------------------------------------------------------------------===//
// Canvas
//===--------------------------------------------------------------------===//
//! Accumulates the space-indented lines of the manual page (bodies word-wrapped to `width`).
class Canvas {
public:
	Canvas(idx_t width, Highlighter highlight) : width(width), highlight(std::move(highlight)) {
	}

	//! A blank line.
	void Blank() {
		lines.emplace_back();
	}

	//! A verbatim line indented `level` levels (section headings, schema-path labels).
	void Line(idx_t level, const string &text) {
		lines.push_back(string(level * INDENT, ' ') + text);
	}

	//! A line centered within the span [indent, width]; widths ignore ANSI codes.
	void Centered(idx_t indent, const string &text) {
		idx_t avail = width > indent ? width - indent : 0;
		idx_t text_width = RenderLength(text);
		idx_t pad = avail > text_width ? (avail - text_width) / 2 : 0;
		lines.push_back(string(indent + pad, ' ') + text);
	}

	//! A header row within [indent, width]: `left` flush-left, `center` centered, `right` flush-right,
	//! with at least `min_gap` spaces between segments. Emits + returns true when it fits.
	bool TryHeaderRow(idx_t indent, const string &left, const string &center, const string &right, idx_t min_gap) {
		idx_t avail = width > indent ? width - indent : 0;
		idx_t lw = RenderLength(left), cw = RenderLength(center), rw = RenderLength(right);
		if (cw >= avail || rw >= avail) {
			return false;
		}
		idx_t center_start = (avail - cw) / 2;
		idx_t right_start = avail - rw;
		if (center_start < lw + min_gap || right_start < center_start + cw + min_gap) {
			return false;
		}
		string line = string(indent, ' ') + left + string(center_start - lw, ' ') + center;
		line += string(right_start - (center_start + cw), ' ') + right;
		lines.push_back(std::move(line));
		return true;
	}

	//! A word-wrapped body block, every line indented `level` levels.
	void Body(idx_t level, const string &text) {
		idx_t indent = level * INDENT;
		auto wrapped = WordWrap(text, Budget(indent));
		if (wrapped.empty()) {
			lines.push_back(string(indent, ' '));
			return;
		}
		for (auto &chunk : wrapped) {
			lines.push_back(string(indent, ' ') + chunk);
		}
	}

	//! A word-wrapped entry whose `marker` sits in the indent gap on the first line, with the text
	//! aligned at `level`; continuation lines are indented to `level`.
	void Item(idx_t level, const string &marker, const string &text) {
		idx_t indent = level * INDENT;
		auto wrapped = WordWrap(text, Budget(indent));
		if (wrapped.empty()) {
			wrapped.emplace_back();
		}
		for (idx_t i = 0; i < wrapped.size(); i++) {
			lines.push_back((i == 0 ? MarkerPrefix(level, marker) : string(indent, ' ')) + wrapped[i]);
		}
	}

	//! A line of syntax-highlighted content indented `level` levels, emitted verbatim (no wrapping) so
	//! it stays copy-pasteable.
	void Highlighted(idx_t level, const string &text) {
		lines.push_back(string(level * INDENT, ' ') + (highlight ? highlight(text) : text));
	}

	//! Highlighted content whose `marker` sits in the indent gap, with the text aligned at `level`.
	void HighlightedItem(idx_t level, const string &marker, const string &text) {
		lines.push_back(MarkerPrefix(level, marker) + (highlight ? highlight(text) : text));
	}

	//! Render the accumulated lines.
	string Render() const {
		string out;
		for (auto &line : lines) {
			out += line;
			out += "\n";
		}
		return out;
	}

private:
	//! The first-line prefix for a marked entry: `marker` placed in the indent gap so the following
	//! text starts at `level` columns (short markers are padded, over-long markers push the text right).
	string MarkerPrefix(idx_t level, const string &marker) const {
		idx_t indent = level * INDENT;
		idx_t marker_indent = level >= 2 ? (level - 2) * INDENT : 0;
		string prefix = string(marker_indent, ' ') + marker;
		while (RenderLength(prefix) < indent) {
			prefix += ' ';
		}
		return prefix;
	}

	//! Word-wrap budget for content sitting at `indent` columns.
	idx_t Budget(idx_t indent) const {
		return width > indent + 4 ? width - indent : 4;
	}

private:
	idx_t width;
	Highlighter highlight;
	vector<string> lines;
};

//===--------------------------------------------------------------------===//
// Grouping / dedup
//===--------------------------------------------------------------------===//
//! An overload paired with the sequential number assigned to it (after grouping by function type).
struct NumberedOverload {
	idx_t number;
	const ManualOverload *overload;
};

//! An ordered group of overloads that share identical content.
struct ContentGroup {
	//! signature numbers sharing this content, in ascending order
	vector<idx_t> numbers;
	//! the (single-string) key/content this group carries
	string text;
	//! the example list this group carries (for the Examples section)
	vector<string> examples;
};

//! Format a group's reference list, e.g. "1. 2. 3.". Runs of more than 5 consecutive numbers collapse
//! to a "first-last." range. Each piece is a space-separated, independently colored (via `colorize`)
//! token so the list can be word-wrapped safely.
string FormatRefs(const vector<idx_t> &numbers, const std::function<string(const string &)> &colorize) {
	vector<string> pieces;
	for (idx_t i = 0; i < numbers.size();) {
		idx_t j = i;
		while (j + 1 < numbers.size() && numbers[j + 1] == numbers[j] + 1) {
			j++;
		}
		if (j - i + 1 > 5) {
			pieces.push_back(std::to_string(numbers[i]) + "-" + std::to_string(numbers[j]) + ".");
		} else {
			for (idx_t k = i; k <= j; k++) {
				pieces.push_back(std::to_string(numbers[k]) + ".");
			}
		}
		i = j + 1;
	}
	string result;
	for (idx_t i = 0; i < pieces.size(); i++) {
		if (i > 0) {
			result += " ";
		}
		result += colorize ? colorize(pieces[i]) : pieces[i];
	}
	return result;
}

//! Bucket overloads into content groups, preserving first-appearance order. `key` returns the grouping
//! key for an overload (empty to skip it); `fill` initializes a freshly-created group's content.
vector<ContentGroup> GroupBy(const vector<NumberedOverload> &overloads,
                             const std::function<string(const ManualOverload &)> &key,
                             const std::function<void(ContentGroup &, const ManualOverload &)> &fill) {
	vector<ContentGroup> groups;
	for (auto &entry : overloads) {
		string group_key = key(*entry.overload);
		if (group_key.empty()) {
			continue;
		}
		bool found = false;
		for (auto &group : groups) {
			if (group.text == group_key) {
				group.numbers.push_back(entry.number);
				found = true;
				break;
			}
		}
		if (!found) {
			ContentGroup group;
			group.numbers.push_back(entry.number);
			group.text = group_key;
			fill(group, *entry.overload);
			groups.push_back(std::move(group));
		}
	}
	return groups;
}

//! Group overloads by their (identical) description text.
vector<ContentGroup> GroupByDescription(const vector<NumberedOverload> &overloads) {
	return GroupBy(
	    overloads, [](const ManualOverload &overload) { return overload.description; },
	    [](ContentGroup &, const ManualOverload &) {});
}

//! Group overloads by their (identical) example list, keyed on the joined examples.
vector<ContentGroup> GroupByExamples(const vector<NumberedOverload> &overloads) {
	return GroupBy(
	    overloads,
	    [](const ManualOverload &overload) {
		    // join with a separator that cannot appear inside a single example to key the group
		    return overload.examples.empty() ? string() : StringUtil::Join(overload.examples, "\n\x01\n");
	    },
	    [](ContentGroup &group, const ManualOverload &overload) { group.examples = overload.examples; });
}

//! Banner label for a raw function type, e.g. "scalar" -> "scalar function",
//! "table_macro" -> "table macro function".
string EntryTypeLabel(const string &function_type) {
	string label = StringUtil::Join(StringUtil::Split(function_type, '_'), " ");
	return label.empty() ? "function" : label + " function";
}

} // namespace

string BuildSignature(const string &name, const vector<string> &parameters, const vector<string> &parameter_types,
                      const string &varargs, const string &return_type, const string &name_color,
                      const string &type_color, const string &color_off) {
	// color each whitespace-separated token independently so that word-wrapping a multi-word type
	// (e.g. "TIMESTAMP WITH TIME ZONE") keeps every wrapped fragment self-contained
	auto colorize = [&](const string &text, const string &color) -> string {
		if (color.empty() || text.empty()) {
			return text;
		}
		auto tokens = StringUtil::Split(text, ' ');
		for (auto &token : tokens) {
			token = color + token + color_off;
		}
		return StringUtil::Join(tokens, " ");
	};

	string result = name + "(";
	for (idx_t i = 0; i < parameter_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		string param_name = i < parameters.size() && !parameters[i].empty() ? parameters[i] : "col" + std::to_string(i);
		result += colorize(param_name, name_color);
		// macros have no declared parameter types, so only append a type when one is present
		if (!parameter_types[i].empty()) {
			result += " " + colorize(parameter_types[i], type_color);
		}
	}
	if (!varargs.empty()) {
		if (!parameter_types.empty()) {
			result += ", ";
		}
		result += colorize(varargs, type_color) + "...";
	}
	result += ")";
	if (!return_type.empty()) {
		result += " -> " + colorize(return_type, type_color);
	}
	return result;
}

string RenderManualPage(const vector<ManualOverload> &overloads, const string &pattern, idx_t content_width,
                        const ManualStyle &style, const ManualHighlighter &highlighter) {
	if (content_width < 24) {
		content_width = 24;
	}
	Canvas canvas(content_width, highlighter);

	// reference markers, rules and the counter are colored with the (gray) layout color
	auto color_ref = [&](const string &text) {
		return style.layout_on.empty() ? text : style.layout_on + text + style.layout_off;
	};
	// a signature/reference marker "n." (gray); MarkerPrefix pads the following content into alignment,
	// leaving room for multi-digit numbers
	auto marker = [&](idx_t n) {
		return color_ref(std::to_string(n) + ".");
	};
	// section headings are upper-cased and emphasized (white + bold)
	auto heading = [&](const string &text) {
		string upper = StringUtil::Upper(text);
		return style.heading_on.empty() ? upper : style.heading_on + upper + style.heading_off;
	};
	// the banner entry name is emphasized (white + bold) but kept in its real case
	auto title = [&](const string &text) {
		return style.heading_on.empty() ? text : style.heading_on + text + style.heading_off;
	};
	// header schema path / entry type styling (white)
	auto path_label = [&](const string &text) {
		return style.path_on.empty() ? text : style.path_on + text + style.path_off;
	};

	// header rules and content are inset one level
	idx_t rule_indent = INDENT;
	idx_t rule_width = content_width > rule_indent ? content_width - rule_indent : 0;

	// split the overloads into frames, one per distinct (name, schema path, function type), in first-
	// appearance order; each frame is rendered as its own self-contained manual entry
	struct Frame {
		string function_name;
		string schema_path;
		string function_type;
		vector<const ManualOverload *> overloads;
	};
	vector<Frame> frames;
	for (auto &overload : overloads) {
		Frame *target = nullptr;
		for (auto &frame : frames) {
			if (frame.function_name == overload.function_name && frame.schema_path == overload.schema_path &&
			    frame.function_type == overload.function_type) {
				target = &frame;
				break;
			}
		}
		if (!target) {
			frames.push_back({overload.function_name, overload.schema_path, overload.function_type, {}});
			target = &frames.back();
		}
		target->overloads.push_back(&overload);
	}

	// each frame opens with a full-width "─" rule that, when several entries are shown, embeds a centered
	// "n / total" position counter
	bool multiple = frames.size() > 1;
	auto frame_rule = [&](idx_t index) {
		string counter = multiple ? " " + std::to_string(index + 1) + " / " + std::to_string(frames.size()) + " " : "";
		string bar;
		if (!counter.empty() && counter.size() + 2 <= rule_width) {
			idx_t left = (rule_width - counter.size()) / 2;
			idx_t right = rule_width - counter.size() - left;
			bar = color_ref(StringUtil::Repeat("\xE2\x94\x80", left) + counter +
			                StringUtil::Repeat("\xE2\x94\x80", right));
		} else {
			bar = color_ref(StringUtil::Repeat("\xE2\x94\x80", rule_width));
		}
		return string(rule_indent, ' ') + bar;
	};

	for (idx_t f = 0; f < frames.size(); f++) {
		auto &frame = frames[f];

		// frame header: a rule (with a "n / total" counter when several entries are shown) above the
		// schema path (left), entry name (centered) and entry type (right); stacked+centered if too wide
		string header_path = path_label(frame.schema_path);
		string header_name = title(frame.function_name);
		string header_type = path_label(EntryTypeLabel(frame.function_type));
		canvas.Line(0, frame_rule(f));
		if (!canvas.TryHeaderRow(rule_indent, header_path, header_name, header_type, 4)) {
			canvas.Centered(rule_indent, header_path);
			canvas.Centered(rule_indent, header_name);
			canvas.Centered(rule_indent, header_type);
		}
		canvas.Blank();

		// number this frame's overloads; group descriptions/examples so numbering can react to them
		vector<NumberedOverload> numbered;
		for (auto *overload : frame.overloads) {
			numbered.push_back({numbered.size() + 1, overload});
		}
		auto description_groups = GroupByDescription(numbered);
		auto example_groups = GroupByExamples(numbered);
		// a single group covering every overload applies to all of them, so its reference is redundant
		auto covers_all = [&](const vector<ContentGroup> &groups) {
			return groups.size() == 1 && groups[0].numbers.size() == numbered.size();
		};
		bool description_refs = !description_groups.empty() && !covers_all(description_groups);
		bool example_refs = !example_groups.empty() && !covers_all(example_groups);
		// the "n." markers only earn their place when a description/example references specific overloads
		bool show_numbers = numbered.size() > 1 && (description_refs || example_refs);

		// the "n." marker sits at level 2 (4 cols) and content aligns at level 4 (8 cols); a multi-
		// reference group lists its numbers on their own line (also level 2), aligned with the marker

		// Signatures; the "n." marker sits in the indent gap on the first line
		canvas.Line(1, heading("Signature"));
		canvas.Blank();
		for (auto &entry : numbered) {
			// the signature is already colored structurally by BuildSignature; do not re-highlight it
			if (show_numbers) {
				canvas.Item(4, marker(entry.number), entry.overload->signature);
			} else {
				canvas.Body(4, entry.overload->signature);
			}
			canvas.Blank();
		}

		// Descriptions: a single-reference group carries its "n." marker inline; multiple references are
		// listed on their own line above the body
		if (!description_groups.empty()) {
			canvas.Line(1, heading("Description"));
			canvas.Blank();
			for (auto &group : description_groups) {
				if (!description_refs) {
					canvas.Body(4, group.text);
				} else if (group.numbers.size() == 1) {
					canvas.Item(4, marker(group.numbers[0]), group.text);
				} else {
					canvas.Body(2, FormatRefs(group.numbers, color_ref));
					canvas.Blank();
					canvas.Body(4, group.text);
				}
				canvas.Blank();
			}
		}

		// Examples: same referencing, over verbatim (syntax-highlighted) SQL that stays copy-pasteable
		if (!example_groups.empty()) {
			canvas.Line(1, heading("Examples"));
			canvas.Blank();
			for (auto &group : example_groups) {
				bool inline_ref = example_refs && group.numbers.size() == 1;
				if (example_refs && group.numbers.size() > 1) {
					canvas.Body(2, FormatRefs(group.numbers, color_ref));
					canvas.Blank();
				}
				bool first_line = true;
				for (idx_t e = 0; e < group.examples.size(); e++) {
					if (e > 0) {
						canvas.Blank();
					}
					for (auto &physical : StringUtil::Split(group.examples[e], '\n')) {
						if (inline_ref && first_line) {
							canvas.HighlightedItem(4, marker(group.numbers[0]), physical);
						} else {
							canvas.Highlighted(4, physical);
						}
						first_line = false;
					}
				}
				canvas.Blank();
			}
		}
	}

	return canvas.Render();
}

} // namespace duckdb_shell
