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

	//! A right-aligned label, flush against the right margin (the "(1), (2)" reference groups).
	void RightAligned(const string &label) {
		idx_t label_width = RenderLength(label);
		lines.push_back(width > label_width ? string(width - label_width, ' ') + label : label);
	}

	//! A horizontally centered line (the banner header lines); widths ignore ANSI codes.
	void Centered(const string &text) {
		idx_t text_width = RenderLength(text);
		idx_t pad = width > text_width ? (width - text_width) / 2 : 0;
		lines.push_back(string(pad, ' ') + text);
	}

	//! A boxed line: `text` centered within the interior (border + space padding on each side) and
	//! framed by the `border` glyph.
	void BoxCentered(const string &border, const string &text) {
		idx_t interior = width > 4 ? width - 4 : 0; // two borders + a space of padding on each side
		idx_t text_width = RenderLength(text);
		idx_t pad = interior > text_width ? interior - text_width : 0;
		idx_t left = pad / 2;
		lines.push_back(border + " " + string(left, ' ') + text + string(pad - left, ' ') + " " + border);
	}

	//! A boxed header row: `left` flush-left, `center` centered, `right` flush-right within the interior
	//! (border + space padding on each side), framed by `border`, requiring at least `min_gap` spaces
	//! between segments. Returns true and emits the line when it fits; otherwise returns false.
	bool TryBoxHeaderRow(const string &border, const string &left, const string &center, const string &right,
	                     idx_t min_gap) {
		idx_t interior = width > 4 ? width - 4 : 0;
		idx_t lw = RenderLength(left), cw = RenderLength(center), rw = RenderLength(right);
		if (cw >= interior || rw >= interior) {
			return false;
		}
		idx_t center_start = (interior - cw) / 2;
		idx_t right_start = interior - rw;
		if (center_start < lw + min_gap || right_start < center_start + cw + min_gap) {
			return false;
		}
		string inner = left + string(center_start - lw, ' ') + center;
		inner += string(right_start - (center_start + cw), ' ') + right;
		lines.push_back(border + " " + inner + " " + border);
		return true;
	}

	//! A signature indented `level` levels, with `number` right-aligned against the margin on the last
	//! line (or on its own line when there is no room). `text`/`number` may contain ANSI color codes.
	void Numbered(idx_t level, const string &text, const string &number) {
		Body(level, text);
		idx_t number_width = RenderLength(number);
		idx_t used = RenderLength(lines.back());
		if (used + 1 + number_width <= width) {
			lines.back() += string(width - used - number_width, ' ') + number;
		} else {
			RightAligned(number);
		}
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

	//! A line of syntax-highlighted content indented `level` levels, emitted verbatim (no wrapping) so
	//! it stays copy-pasteable.
	void Highlighted(idx_t level, const string &text) {
		lines.push_back(string(level * INDENT, ' ') + (highlight ? highlight(text) : text));
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

//! Format a group's reference header, e.g. "(1), (2), (3)". Runs of more than 5 consecutive numbers
//! are collapsed to a "(first-last)" range. Each piece is a space-separated, independently colored (via
//! `colorize`) token so the header can be word-wrapped safely.
string FormatRefs(const vector<idx_t> &numbers, const std::function<string(const string &)> &colorize) {
	// build the pieces, collapsing each maximal run of > 3 consecutive numbers into "(first-last)"
	vector<string> pieces;
	for (idx_t i = 0; i < numbers.size();) {
		idx_t j = i;
		while (j + 1 < numbers.size() && numbers[j + 1] == numbers[j] + 1) {
			j++;
		}
		if (j - i + 1 > 5) {
			pieces.push_back("(" + std::to_string(numbers[i]) + "-" + std::to_string(numbers[j]) + ")");
		} else {
			for (idx_t k = i; k <= j; k++) {
				pieces.push_back("(" + std::to_string(numbers[k]) + ")");
			}
		}
		i = j + 1;
	}
	string result;
	for (idx_t i = 0; i < pieces.size(); i++) {
		if (i > 0) {
			result += " ";
		}
		string token = i + 1 < pieces.size() ? pieces[i] + "," : pieces[i];
		result += colorize ? colorize(token) : token;
	}
	return result;
}

//! Bucket overloads by a string key, preserving first-appearance order and dropping empty content.
vector<ContentGroup> GroupByDescription(const vector<NumberedOverload> &overloads) {
	vector<ContentGroup> groups;
	for (auto &entry : overloads) {
		if (entry.overload->description.empty()) {
			continue;
		}
		bool found = false;
		for (auto &group : groups) {
			if (group.text == entry.overload->description) {
				group.numbers.push_back(entry.number);
				found = true;
				break;
			}
		}
		if (!found) {
			ContentGroup group;
			group.numbers.push_back(entry.number);
			group.text = entry.overload->description;
			groups.push_back(std::move(group));
		}
	}
	return groups;
}

//! Bucket overloads by their example list, preserving first-appearance order and dropping empties.
vector<ContentGroup> GroupByExamples(const vector<NumberedOverload> &overloads) {
	vector<ContentGroup> groups;
	for (auto &entry : overloads) {
		if (entry.overload->examples.empty()) {
			continue;
		}
		// join with a separator that cannot appear inside a single example to key the group
		string key = StringUtil::Join(entry.overload->examples, "\n\x01\n");
		bool found = false;
		for (auto &group : groups) {
			if (group.text == key) {
				group.numbers.push_back(entry.number);
				found = true;
				break;
			}
		}
		if (!found) {
			ContentGroup group;
			group.numbers.push_back(entry.number);
			group.text = key;
			group.examples = entry.overload->examples;
			groups.push_back(std::move(group));
		}
	}
	return groups;
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
                        const string &layout_on, const string &layout_off, const string &heading_on,
                        const string &heading_off, const string &path_on, const string &path_off,
                        const ManualHighlighter &highlighter) {
	if (content_width < 24) {
		content_width = 24;
	}
	Canvas canvas(content_width, highlighter);

	// reference markers like "(1)" are colored with the (gray) layout color
	auto color_ref = [&](const string &refs) {
		return layout_on.empty() ? refs : layout_on + refs + layout_off;
	};
	// section headings are upper-cased and emphasized (white + bold)
	auto heading = [&](const string &text) {
		string upper = StringUtil::Upper(text);
		return heading_on.empty() ? upper : heading_on + upper + heading_off;
	};
	// the banner entry name is emphasized (white + bold) but kept in its real case
	auto title = [&](const string &text) {
		return heading_on.empty() ? text : heading_on + text + heading_off;
	};
	// schema-path labels are de-emphasized (gray + italic)
	auto path_label = [&](const string &text) {
		return path_on.empty() ? text : path_on + text + path_off;
	};

	// header box pieces (rounded corners): ╭ ╮ ╰ ╯ │, with a full-width top/bottom bar
	string box_bar = StringUtil::Repeat("\xE2\x94\x80", content_width > 2 ? content_width - 2 : 0);
	string box_top = color_ref(string("\xE2\x95\xAD") + box_bar + "\xE2\x95\xAE");
	string box_bottom = color_ref(string("\xE2\x95\xB0") + box_bar + "\xE2\x95\xAF");
	string box_side = color_ref("\xE2\x94\x82");

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

	// Now render
	string summary = "found " + std::to_string(frames.size()) + " entries matching '" + pattern + "'";
	if (frames.size() > 1) {
		canvas.Blank();
		canvas.Centered(path_label(summary));
		canvas.Blank();
	}

	for (idx_t f = 0; f < frames.size(); f++) {
		auto &frame = frames[f];

		// frame header, boxed: schema path (gray) left, entry name (bold) centered, entry type (gray)
		// right - on one line when they fit with a 4-space gap, otherwise stacked and centered
		string header_path = path_label(frame.schema_path);
		string header_name = title(frame.function_name);
		string header_type = color_ref(EntryTypeLabel(frame.function_type));
		canvas.Line(0, box_top);
		if (!canvas.TryBoxHeaderRow(box_side, header_path, header_name, header_type, 4)) {
			canvas.BoxCentered(box_side, header_path);
			canvas.BoxCentered(box_side, header_name);
			canvas.BoxCentered(box_side, header_type);
		}
		canvas.Line(0, box_bottom);
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
		// the "(n)" markers only earn their place when a description/example references specific overloads
		bool show_numbers = numbered.size() > 1 && (description_refs || example_refs);

		// Signatures; the "(n)" marker is right-aligned at the margin
		canvas.Line(1, heading("Signature"));
		canvas.Blank();
		for (auto &entry : numbered) {
			// the signature is already colored structurally by BuildSignature; do not re-highlight it
			if (show_numbers) {
				canvas.Numbered(3, entry.overload->signature, color_ref("(" + std::to_string(entry.number) + ")"));
			} else {
				canvas.Body(3, entry.overload->signature);
			}
			canvas.Blank();
		}

		// Descriptions: the reference group left-aligned above the body on the line(s) below
		if (!description_groups.empty()) {
			canvas.Line(1, heading("Description"));
			canvas.Blank();
			for (auto &group : description_groups) {
				if (description_refs) {
					canvas.Body(2, FormatRefs(group.numbers, color_ref));
					canvas.Blank();
				}
				canvas.Body(3, group.text);
				canvas.Blank();
			}
		}

		// Examples: the reference group left-aligned above the (syntax-highlighted) SQL, verbatim and
		// un-boxed so it can be copy-pasted directly from the terminal
		if (!example_groups.empty()) {
			canvas.Line(1, heading("Examples"));
			canvas.Blank();
			for (auto &group : example_groups) {
				if (example_refs) {
					canvas.Body(2, FormatRefs(group.numbers, color_ref));
					canvas.Blank();
				}
				for (idx_t e = 0; e < group.examples.size(); e++) {
					if (e > 0) {
						canvas.Blank();
					}
					for (auto &physical : StringUtil::Split(group.examples[e], '\n')) {
						canvas.Highlighted(3, physical);
					}
				}
				canvas.Blank();
			}
		}
	}

	// footer: a rule and a summary of how many entries were shown (only when there is more than one)
	if (frames.size() > 1) {
		canvas.Blank();
		canvas.Centered(path_label(summary));
		canvas.Blank();
	}

	return canvas.Render();
}

} // namespace duckdb_shell
