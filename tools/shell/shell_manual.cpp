#include "shell_manual.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

#include <functional>

namespace duckdb_shell {

using duckdb::StringUtil;
using duckdb::Utf8Proc;

namespace {

//===--------------------------------------------------------------------===//
// Box-drawing glyphs (with ASCII fallback)
//===--------------------------------------------------------------------===//
struct Glyphs {
#ifndef DUCKDB_ASCII_TREE_RENDERER
	static constexpr auto *LTCORNER = "\342\225\255";  // "╭"
	static constexpr auto *RTCORNER = "\342\225\256";  // "╮"
	static constexpr auto *LDCORNER = "\342\225\260";  // "╰"
	static constexpr auto *RDCORNER = "\342\225\257";  // "╯"
	static constexpr auto *LMIDDLE = "\342\224\234";   // "├"
	static constexpr auto *RMIDDLE = "\342\224\244";   // "┤"
	static constexpr auto *VERTICAL = "\342\224\202";  // "│"
	static constexpr auto *HORIZONTAL = "\342\224\200"; // "─"
#else
	static constexpr const char *LTCORNER = "+";
	static constexpr const char *RTCORNER = "+";
	static constexpr const char *LDCORNER = "+";
	static constexpr const char *RDCORNER = "+";
	static constexpr const char *LMIDDLE = "+";
	static constexpr const char *RMIDDLE = "+";
	static constexpr const char *VERTICAL = "|";
	static constexpr const char *HORIZONTAL = "-";
#endif
};

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

//===--------------------------------------------------------------------===//
// Box canvas
//===--------------------------------------------------------------------===//
//! Accumulates the interior content lines of a box (each exactly `width` display columns) and
//! renders them wrapped in a bordered frame with a centered title/subtitle.
class BoxCanvas {
public:
	BoxCanvas(idx_t width, string layout_on, string layout_off, Highlighter highlight)
	    : width(width), layout_on(std::move(layout_on)), layout_off(std::move(layout_off)),
	      highlight(std::move(highlight)) {
	}

	//! A blank interior line.
	void Blank() {
		lines.emplace_back();
	}

	//! A left-aligned interior line indented by `indent` columns; text is padded/truncated to fit.
	void Line(idx_t indent, const string &text) {
		string content(indent, ' ');
		content += text;
		lines.push_back(std::move(content));
	}

	//! Emit `text` wrapped to the interior, with `first_indent` on the first line and `hang_indent`
	//! on continuation lines. A non-empty `label` is written left-aligned at `first_indent`. `text`
	//! may already contain ANSI color codes (widths are measured ignoring them).
	void Wrapped(idx_t first_indent, idx_t hang_indent, const string &label, const string &text) {
		idx_t body_indent = hang_indent;
		idx_t budget = width > body_indent + 1 ? width - body_indent : 2;
		auto wrapped = WordWrap(text, budget);
		for (idx_t i = 0; i < wrapped.size(); i++) {
			idx_t indent = i == 0 ? first_indent : hang_indent;
			string content(indent, ' ');
			if (i == 0 && !label.empty()) {
				content += label;
			}
			// pad up to the body indent so continuation lines align under the text
			while (RenderLength(content) < hang_indent) {
				content += ' ';
			}
			content += wrapped[i];
			lines.push_back(std::move(content));
		}
	}

	//! Draw a nested box at `indent`, containing the given (already newline-split) text lines.
	//! `top_prefix`, if given, replaces the indent on the top-border row (e.g. a "(1)  " label) and
	//! must render to exactly `indent` columns so the box stays aligned.
	void NestedBox(idx_t indent, const vector<string> &body, const string &top_prefix = string()) {
		idx_t interior = width > indent * 2 + 2 ? width - indent * 2 - 2 : 4;
		string pad(indent, ' ');
		string bar = StringUtil::Repeat(Glyphs::HORIZONTAL, interior);
		lines.push_back((top_prefix.empty() ? pad : top_prefix) + Glyphs::LTCORNER + bar + Glyphs::RTCORNER);
		auto inner_line = [&](const string &text) {
			string t = text;
			idx_t budget = interior > 2 ? interior - 2 : 0;
			if (RenderLength(t) > budget) {
				t = TruncateText(t, budget);
			}
			idx_t visible = RenderLength(t);
			// highlight the (plain, already-truncated) fragment; width math uses the plain width
			string s = pad + Glyphs::VERTICAL + "  " + (highlight ? highlight(t) : t);
			for (idx_t filled = 2 + visible; filled < interior; filled++) {
				s += ' ';
			}
			s += Glyphs::VERTICAL;
			lines.push_back(std::move(s));
		};
		idx_t budget = interior > 3 ? interior - 3 : 2;
		inner_line("");
		for (auto &text : body) {
			if (text.empty()) {
				inner_line("");
				continue;
			}
			// word-wrap long example lines to the inner box width instead of truncating
			for (auto &wrapped : WordWrap(text, budget)) {
				inner_line(wrapped);
			}
		}
		inner_line("");
		lines.push_back(pad + Glyphs::LDCORNER + bar + Glyphs::RDCORNER);
	}

	//! Render the accumulated lines into the final bordered frame string. `title` is embedded in the
	//! top border like the EXPLAIN output: "╭─ title ─────╮".
	string Render(const string &title) const {
		string out;
		Emit(out, TopBorder(title));
		for (auto &line : lines) {
			Emit(out, Border(line));
		}
		Emit(out, Glyphs::LDCORNER + StringUtil::Repeat(Glyphs::HORIZONTAL, width) + Glyphs::RDCORNER);
		return out;
	}

private:
	//! Build the top border with an inline title: "╭─ title " followed by fill dashes and "╮".
	string TopBorder(const string &title) const {
		string t = title;
		// keep at least three trailing dashes; truncate an over-long title to fit
		idx_t max_title = width > 6 ? width - 6 : 0;
		if (RenderLength(t) > max_title) {
			t = TruncateText(t, max_title);
		}
		// interior = "─ " + title + " " + fill dashes  == width columns
		idx_t used = 3 + RenderLength(t);
		idx_t fill = width > used ? width - used : 0;
		return string(Glyphs::LTCORNER) + Glyphs::HORIZONTAL + " " + t + " " +
		       StringUtil::Repeat(Glyphs::HORIZONTAL, fill) + Glyphs::RTCORNER;
	}

	//! Colorize the box-drawing glyph runs in `line` and append it (with newline) to `out`.
	void Emit(string &out, const string &line) const {
		out += Colorize(line);
		out += "\n";
	}

	//! Wrap maximal runs of box-drawing glyphs in the layout terminal codes. Content text does not
	//! contain these glyphs, so a simple scan is safe.
	string Colorize(const string &line) const {
		if (layout_on.empty()) {
			return line;
		}
		static constexpr const char *const box_glyphs[] = {Glyphs::LTCORNER, Glyphs::RTCORNER, Glyphs::LDCORNER,
		                                                   Glyphs::RDCORNER, Glyphs::LMIDDLE,  Glyphs::RMIDDLE,
		                                                   Glyphs::VERTICAL, Glyphs::HORIZONTAL};
		string result;
		bool in_run = false;
		for (idx_t i = 0; i < line.size();) {
			idx_t glyph_len = 0;
			for (auto *glyph : box_glyphs) {
				idx_t len = string(glyph).size();
				if (line.compare(i, len, glyph) == 0) {
					glyph_len = len;
					break;
				}
			}
			if (glyph_len > 0) {
				if (!in_run) {
					result += layout_on;
					in_run = true;
				}
				result.append(line, i, glyph_len);
				i += glyph_len;
			} else {
				if (in_run) {
					result += layout_off;
					in_run = false;
				}
				result += line[i];
				i++;
			}
		}
		if (in_run) {
			result += layout_off;
		}
		return result;
	}

	//! Wrap an interior line (padding/truncating to exactly `width`) in vertical borders.
	string Border(const string &content) const {
		string t = content;
		if (RenderLength(t) > width) {
			t = TruncateText(t, width);
		}
		string padded = t;
		while (RenderLength(padded) < width) {
			padded += ' ';
		}
		return string(Glyphs::VERTICAL) + padded + Glyphs::VERTICAL;
	}

private:
	idx_t width;
	string layout_on;
	string layout_off;
	Highlighter highlight;
	vector<string> lines;
};

//===--------------------------------------------------------------------===//
// Grouping / dedup
//===--------------------------------------------------------------------===//
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
vector<ContentGroup> GroupByDescription(const vector<ManualOverload> &overloads) {
	vector<ContentGroup> groups;
	for (auto &overload : overloads) {
		if (overload.description.empty()) {
			continue;
		}
		bool found = false;
		for (auto &group : groups) {
			if (group.text == overload.description) {
				group.numbers.push_back(overload.number);
				found = true;
				break;
			}
		}
		if (!found) {
			ContentGroup group;
			group.numbers.push_back(overload.number);
			group.text = overload.description;
			groups.push_back(std::move(group));
		}
	}
	return groups;
}

//! Bucket overloads by their example list, preserving first-appearance order and dropping empties.
vector<ContentGroup> GroupByExamples(const vector<ManualOverload> &overloads) {
	vector<ContentGroup> groups;
	for (auto &overload : overloads) {
		if (overload.examples.empty()) {
			continue;
		}
		// join with a separator that cannot appear inside a single example to key the group
		string key = StringUtil::Join(overload.examples, "\n\x01\n");
		bool found = false;
		for (auto &group : groups) {
			if (group.text == key) {
				group.numbers.push_back(overload.number);
				found = true;
				break;
			}
		}
		if (!found) {
			ContentGroup group;
			group.numbers.push_back(overload.number);
			group.text = key;
			group.examples = overload.examples;
			groups.push_back(std::move(group));
		}
	}
	return groups;
}

// Interior layout indents (in display columns)
constexpr idx_t HEADER_INDENT = 1;
constexpr idx_t ENTRY_INDENT = 3;
constexpr idx_t BODY_INDENT = 7;

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

string RenderManualPage(const string &title, const vector<ManualOverload> &overloads, idx_t content_width,
                        const string &layout_on, const string &layout_off, const ManualHighlighter &highlighter) {
	if (content_width < 24) {
		content_width = 24;
	}
	BoxCanvas canvas(content_width, layout_on, layout_off, highlighter);

	// reference markers like "(1)" are colored with the (gray) layout color
	auto color_ref = [&](const string &refs) { return layout_on.empty() ? refs : layout_on + refs + layout_off; };

	// Signatures
	canvas.Blank();
	canvas.Line(HEADER_INDENT, "Signatures");
	canvas.Blank();
	for (auto &overload : overloads) {
		string label = color_ref("(" + std::to_string(overload.number) + ")") + " ";
		// the signature is already colored structurally by BuildSignature; do not re-highlight it
		canvas.Wrapped(ENTRY_INDENT, ENTRY_INDENT + RenderLength(label), label, overload.signature);
		canvas.Blank();
	}

	// Description
	auto description_groups = GroupByDescription(overloads);
	if (!description_groups.empty()) {
		canvas.Line(HEADER_INDENT, "Description");
		canvas.Blank();
		for (auto &group : description_groups) {
			if (group.numbers.size() == 1) {
				// single overload: put the description on the same line as the reference
				string label = color_ref(FormatRefs(group.numbers)) + " ";
				canvas.Wrapped(ENTRY_INDENT, ENTRY_INDENT + RenderLength(label), label, group.text);
			} else {
				canvas.Line(ENTRY_INDENT, color_ref(FormatRefs(group.numbers)));
				canvas.Blank();
				canvas.Wrapped(BODY_INDENT, BODY_INDENT, "", group.text);
			}
			canvas.Blank();
		}
	}

	// Examples
	auto example_groups = GroupByExamples(overloads);
	if (!example_groups.empty()) {
		canvas.Line(HEADER_INDENT, "Examples");
		canvas.Blank();
		for (auto &group : example_groups) {
			// split each example on newlines so multi-line SQL renders line-by-line
			vector<string> body;
			for (idx_t e = 0; e < group.examples.size(); e++) {
				if (e > 0) {
					body.emplace_back();
				}
				for (auto &physical : StringUtil::Split(group.examples[e], '\n')) {
					body.push_back(physical);
				}
			}
			if (group.numbers.size() == 1) {
				// single overload: put the reference on the same line as the box's top border
				string prefix = string(ENTRY_INDENT, ' ') + color_ref(FormatRefs(group.numbers)) + " ";
				canvas.NestedBox(RenderLength(prefix), body, prefix);
			} else {
				canvas.Line(ENTRY_INDENT, color_ref(FormatRefs(group.numbers)));
				canvas.Blank();
				canvas.NestedBox(BODY_INDENT, body);
			}
			canvas.Blank();
		}
	}

	return canvas.Render(title);
}

} // namespace duckdb_shell
