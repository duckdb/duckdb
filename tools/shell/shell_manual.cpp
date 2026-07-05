#include "shell_manual.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
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

	//! A verbatim line at column 0 (section headings).
	void Line(const string &text) {
		lines.push_back(text);
	}

	//! A right-aligned label, flush against the right margin (the "(1), (2)" reference groups).
	void RightAligned(const string &label) {
		idx_t label_width = RenderLength(label);
		lines.push_back(width > label_width ? string(width - label_width, ' ') + label : label);
	}

	//! A signature indented one level, with `number` right-aligned against the margin on the last line
	//! (or on its own line when there is no room). `text`/`number` may contain ANSI color codes.
	void Numbered(const string &text, const string &number) {
		Body(text);
		idx_t number_width = RenderLength(number);
		idx_t used = RenderLength(lines.back());
		if (used + 1 + number_width <= width) {
			lines.back() += string(width - used - number_width, ' ') + number;
		} else {
			RightAligned(number);
		}
	}

	//! A word-wrapped body block, every line indented one level.
	void Body(const string &text) {
		auto wrapped = WordWrap(text, BodyBudget());
		if (wrapped.empty()) {
			lines.push_back(string(INDENT, ' '));
			return;
		}
		for (auto &chunk : wrapped) {
			lines.push_back(string(INDENT, ' ') + chunk);
		}
	}

	//! A line of syntax-highlighted content indented one level, emitted verbatim (no wrapping) so it
	//! stays copy-pasteable.
	void Highlighted(const string &text) {
		lines.push_back(string(INDENT, ' ') + (highlight ? highlight(text) : text));
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
	//! Word-wrap budget for content sitting at one indentation level.
	idx_t BodyBudget() const {
		return width > INDENT + 4 ? width - INDENT : 4;
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

//! Format a group's reference header, e.g. "(1), (2), (3)".
string FormatRefs(const vector<idx_t> &numbers) {
	string result;
	for (idx_t i = 0; i < numbers.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += "(" + std::to_string(numbers[i]) + ")";
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

//! Human-readable heading for a raw function type, e.g. "scalar" -> "Scalar Functions",
//! "table_macro" -> "Table Macro Functions". The "type" sentinel (from duckdb_types()) -> "Types".
string FunctionTypeHeading(const string &function_type) {
	if (function_type == "type") {
		return "Logical Types";
	}
	auto words = StringUtil::Split(function_type, '_');
	for (auto &word : words) {
		if (!word.empty()) {
			word[0] = StringUtil::CharacterToUpper(word[0]);
		}
	}
	string label = StringUtil::Join(words, " ");
	return label.empty() ? "Functions" : label + " Functions";
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
		result += colorize(param_name, name_color) + " " + colorize(parameter_types[i], type_color);
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

string RenderManualPage(const vector<ManualOverload> &overloads, idx_t content_width, const string &layout_on,
                        const string &layout_off, const string &heading_on, const string &heading_off,
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

	// group overloads by function type (first-appearance order) and number them in that grouped order
	vector<string> type_order;
	for (auto &overload : overloads) {
		if (std::find(type_order.begin(), type_order.end(), overload.function_type) == type_order.end()) {
			type_order.push_back(overload.function_type);
		}
	}
	vector<NumberedOverload> numbered;
	for (auto &type : type_order) {
		for (auto &overload : overloads) {
			if (overload.function_type == type) {
				numbered.push_back({numbered.size() + 1, &overload});
			}
		}
	}

	// with a single entry the "(n)" markers are noise: omit them everywhere
	bool show_numbers = numbered.size() > 1;

	// Signatures, under a heading per function type; the "(n)" marker is right-aligned at the margin
	for (auto &type : type_order) {
		canvas.Line(heading(FunctionTypeHeading(type)));
		canvas.Blank();
		for (auto &entry : numbered) {
			if (entry.overload->function_type != type) {
				continue;
			}
			// the signature is already colored structurally by BuildSignature; do not re-highlight it
			if (show_numbers) {
				canvas.Numbered(entry.overload->signature, color_ref("(" + std::to_string(entry.number) + ")"));
			} else {
				canvas.Body(entry.overload->signature);
			}
			canvas.Blank();
		}
	}

	// Descriptions: the reference group right-aligned, then the body on the line(s) below
	auto description_groups = GroupByDescription(numbered);
	if (!description_groups.empty()) {
		canvas.Line(heading("Descriptions"));
		for (auto &group : description_groups) {
			if (show_numbers) {
				canvas.RightAligned(color_ref(FormatRefs(group.numbers)));
			}
			canvas.Body(group.text);
			canvas.Blank();
		}
	}

	// Examples: the reference group right-aligned, then the (syntax-highlighted) SQL below, verbatim
	// and un-boxed so it can be copy-pasted directly from the terminal
	auto example_groups = GroupByExamples(numbered);
	if (!example_groups.empty()) {
		canvas.Line(heading("Examples"));
		for (auto &group : example_groups) {
			if (show_numbers) {
				canvas.RightAligned(color_ref(FormatRefs(group.numbers)));
			}
			for (idx_t e = 0; e < group.examples.size(); e++) {
				if (e > 0) {
					canvas.Blank();
				}
				for (auto &physical : StringUtil::Split(group.examples[e], '\n')) {
					canvas.Highlighted(physical);
				}
			}
			canvas.Blank();
		}
	}

	return canvas.Render();
}

} // namespace duckdb_shell
