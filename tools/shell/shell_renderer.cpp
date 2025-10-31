#include "shell_renderer.hpp"

#include "shell_state.hpp"
#include <stdexcept>
#include <cstring>

namespace duckdb_shell {

bool ShellRenderer::IsColumnar(RenderMode mode) {
	switch (mode) {
	case RenderMode::COLUMN:
	case RenderMode::TABLE:
	case RenderMode::BOX:
	case RenderMode::MARKDOWN:
	case RenderMode::LATEX:
		return true;
	default:
		return false;
	}
}

ShellRenderer::ShellRenderer(ShellState &state)
    : state(state), show_header(state.showHeader), col_sep(state.colSeparator), row_sep(state.rowSeparator) {
}

//===--------------------------------------------------------------------===//
// Column Renderers
//===--------------------------------------------------------------------===//
ColumnRenderer::ColumnRenderer(ShellState &state) : ShellRenderer(state) {
}

string ColumnRenderer::ConvertValue(const char *value) {
	return value ? value : state.nullValue;
}

void ColumnRenderer::RenderFooter(ColumnarResult &result) {
}

void ColumnRenderer::RenderAlignedValue(ColumnarResult &result, idx_t i) {
	idx_t w = result.column_width[i];
	idx_t n = state.RenderLength(result.data[i]);
	state.PrintPadded("", (w - n) / 2);
	state.Print(result.data[i]);
	state.PrintPadded("", (w - n + 1) / 2);
}

class ModeColumnRenderer : public ColumnRenderer {
public:
	explicit ModeColumnRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(ColumnarResult &result) override {
		if (!show_header) {
			return;
		}
		for (idx_t i = 0; i < result.column_count; i++) {
			state.UTF8WidthPrint(result.column_width[i], result.data[i], result.right_align[i]);
			state.Print(i == result.column_count - 1 ? "\n" : "  ");
		}
		for (idx_t i = 0; i < result.column_count; i++) {
			state.PrintDashes(result.column_width[i]);
			state.Print(i == result.column_count - 1 ? "\n" : "  ");
		}
	}

	const char *GetColumnSeparator() override {
		return "  ";
	}
	const char *GetRowSeparator() override {
		return "\n";
	}
};

class ModeTableRenderer : public ColumnRenderer {
public:
	explicit ModeTableRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(ColumnarResult &result) override {
		state.PrintRowSeparator(result.column_count, "+", result.column_width);
		state.Print("| ");
		for (idx_t i = 0; i < result.column_count; i++) {
			RenderAlignedValue(result, i);
			state.Print(i == result.column_count - 1 ? " |\n" : " | ");
		}
		state.PrintRowSeparator(result.column_count, "+", result.column_width);
	}

	void RenderFooter(ColumnarResult &result) override {
		state.PrintRowSeparator(result.column_count, "+", result.column_width);
	}

	const char *GetColumnSeparator() override {
		return " | ";
	}
	const char *GetRowSeparator() override {
		return " |\n";
	}
	const char *GetRowStart() override {
		return "| ";
	}
};

class ModeMarkdownRenderer : public ColumnRenderer {
public:
	explicit ModeMarkdownRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	string ConvertValue(const char *value) override {
		// when rendering for markdown we need to escape pipes
		string result = ColumnRenderer::ConvertValue(value);
		return StringUtil::Replace(result, "|", "\\|");
	}

	void RenderHeader(ColumnarResult &result) override {
		state.Print(GetRowStart());
		for (idx_t i = 0; i < result.column_count; i++) {
			if (i > 0) {
				state.Print(GetColumnSeparator());
			}
			RenderAlignedValue(result, i);
		}
		state.Print(GetRowSeparator());
		state.PrintMarkdownSeparator(result.column_count, "|", result.types, result.column_width);
	}

	const char *GetColumnSeparator() override {
		return " | ";
	}
	const char *GetRowSeparator() override {
		return " |\n";
	}
	const char *GetRowStart() override {
		return "| ";
	}
};

/*
** UTF8 box-drawing characters.  Imagine box lines like this:
**
**           1
**           |
**       4 --+-- 2
**           |
**           3
**
** Each box characters has between 2 and 4 of the lines leading from
** the center.  The characters are here identified by the numbers of
** their corresponding lines.
*/
#define BOX_24   "\342\224\200" /* U+2500 --- */
#define BOX_13   "\342\224\202" /* U+2502  |  */
#define BOX_23   "\342\224\214" /* U+250c  ,- */
#define BOX_34   "\342\224\220" /* U+2510 -,  */
#define BOX_12   "\342\224\224" /* U+2514  '- */
#define BOX_14   "\342\224\230" /* U+2518 -'  */
#define BOX_123  "\342\224\234" /* U+251c  |- */
#define BOX_134  "\342\224\244" /* U+2524 -|  */
#define BOX_234  "\342\224\254" /* U+252c -,- */
#define BOX_124  "\342\224\264" /* U+2534 -'- */
#define BOX_1234 "\342\224\274" /* U+253c -|- */

class ModeBoxRenderer : public ColumnRenderer {
public:
	explicit ModeBoxRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	string ConvertValue(const char *value) override {
		// for MODE_Box truncate large values
		if (!value) {
			return ColumnRenderer::ConvertValue(value);
		}
		static constexpr idx_t MAX_SIZE = 80;
		string result;
		idx_t count = 0;
		bool interrupted = false;
		for (const char *s = value; *s; s++) {
			if (*s == '\n') {
				result += "\\";
				result += "n";
			} else {
				result += *s;
			}
			count++;
			if (count >= MAX_SIZE && ((*s & 0xc0) != 0x80)) {
				interrupted = true;
				break;
			}
		}
		if (interrupted) {
			result += "...";
		}
		return result;
	}

	void RenderHeader(ColumnarResult &result) override {
		print_box_row_separator(result.column_count, BOX_23, BOX_234, BOX_34, result.column_width);
		state.Print(BOX_13 " ");
		for (idx_t i = 0; i < result.column_count; i++) {
			RenderAlignedValue(result, i);
			state.Print(i == result.column_count - 1 ? " " BOX_13 "\n" : " " BOX_13 " ");
		}
		print_box_row_separator(result.column_count, BOX_123, BOX_1234, BOX_134, result.column_width);
	}

	void RenderFooter(ColumnarResult &result) override {
		print_box_row_separator(result.column_count, BOX_12, BOX_124, BOX_14, result.column_width);
	}

	const char *GetColumnSeparator() override {
		return " " BOX_13 " ";
	}
	const char *GetRowSeparator() override {
		return " " BOX_13 "\n";
	}
	const char *GetRowStart() override {
		return BOX_13 " ";
	}

private:
	/* Draw horizontal line N characters long using unicode box
	** characters
	*/
	void print_box_line(idx_t N) {
		string box_line;
		for (idx_t i = 0; i < N; i++) {
			box_line += BOX_24;
		}
		state.Print(box_line);
	}

	/*
	** Draw a horizontal separator for a RenderMode::Box table.
	*/
	void print_box_row_separator(int nArg, const char *zSep1, const char *zSep2, const char *zSep3,
	                             const vector<idx_t> &actualWidth) {
		int i;
		if (nArg > 0) {
			state.Print(zSep1);
			print_box_line(actualWidth[0] + 2);
			for (i = 1; i < nArg; i++) {
				state.Print(zSep2);
				print_box_line(actualWidth[i] + 2);
			}
			state.Print(zSep3);
		}
		state.Print("\n");
	}
};

class ModeLatexRenderer : public ColumnRenderer {
public:
	explicit ModeLatexRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(ColumnarResult &result) override {
		state.Print("\\begin{tabular}{|");
		for (idx_t i = 0; i < result.column_count; i++) {
			if (state.ColumnTypeIsInteger(result.type_names[i].c_str())) {
				state.Print("r");
			} else {
				state.Print("l");
			}
		}
		state.Print("|}\n");
		state.Print("\\hline\n");
		for (idx_t i = 0; i < result.column_count; i++) {
			RenderAlignedValue(result, i);
			state.Print(i == result.column_count - 1 ? GetRowSeparator() : GetColumnSeparator());
		}
		state.Print("\\hline\n");
	}

	void RenderFooter(ColumnarResult &) override {
		state.Print("\\hline\n");
		state.Print("\\end{tabular}\n");
	}

	const char *GetColumnSeparator() override {
		return " & ";
	}
	const char *GetRowSeparator() override {
		return " \\\\\n";
	}
};

unique_ptr<ColumnRenderer> ShellState::GetColumnRenderer() {
	switch (cMode) {
	case RenderMode::COLUMN:
		return unique_ptr<ColumnRenderer>(new ModeColumnRenderer(*this));
	case RenderMode::TABLE:
		return unique_ptr<ColumnRenderer>(new ModeTableRenderer(*this));
	case RenderMode::MARKDOWN:
		return unique_ptr<ColumnRenderer>(new ModeMarkdownRenderer(*this));
	case RenderMode::BOX:
		return unique_ptr<ColumnRenderer>(new ModeBoxRenderer(*this));
	case RenderMode::LATEX:
		return unique_ptr<ColumnRenderer>(new ModeLatexRenderer(*this));
	default:
		throw std::runtime_error("Unsupported mode for GetColumnRenderer");
	}
}

//===--------------------------------------------------------------------===//
// Row Renderers
//===--------------------------------------------------------------------===//
RowRenderer::RowRenderer(ShellState &state) : ShellRenderer(state) {
}

void RowRenderer::Render(RowResult &result) {
	if (first_row) {
		RenderHeader(result);
		first_row = false;
	}
	RenderRow(result);
}

void RowRenderer::RenderHeader(RowResult &result) {
}

void RowRenderer::RenderFooter(RowResult &result) {
}

string RowRenderer::NullValue() {
	return state.nullValue;
}

class ModeLineRenderer : public RowRenderer {
public:
	explicit ModeLineRenderer(ShellState &state) : RowRenderer(state) {
	}

	void Render(RowResult &result) override {
		if (first_row) {
			auto &col_names = result.column_names;
			// determine the render width by going over the column names
			header_width = 5;
			for (idx_t i = 0; i < col_names.size(); i++) {
				auto len = col_names[i].size();
				if (len > header_width) {
					header_width = len;
				}
			}
			first_row = false;
		} else {
			state.Print(state.rowSeparator);
		}
		// render the row
		RenderRow(result);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < data.size(); i++) {
			state.PrintPadded(col_names[i].c_str(), header_width);
			state.Print(" = ");
			state.Print(data[i].c_str());
			state.Print(state.rowSeparator);
		}
	}

	idx_t header_width = 0;
};

class ModeExplainRenderer : public RowRenderer {
public:
	explicit ModeExplainRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		if (data.size() != 2) {
			return;
		}
		if (duckdb::StringUtil::Equals(data[0], "logical_plan") || duckdb::StringUtil::Equals(data[0], "logical_opt") ||
		    duckdb::StringUtil::Equals(data[0], "physical_plan")) {
			state.Print("\n┌─────────────────────────────┐\n");
			state.Print("│┌───────────────────────────┐│\n");
			if (duckdb::StringUtil::Equals(data[0], "logical_plan")) {
				state.Print("││ Unoptimized Logical Plan  ││\n");
			} else if (duckdb::StringUtil::Equals(data[0], "logical_opt")) {
				state.Print("││  Optimized Logical Plan   ││\n");
			} else if (duckdb::StringUtil::Equals(data[0], "physical_plan")) {
				state.Print("││       Physical Plan       ││\n");
			}
			state.Print("│└───────────────────────────┘│\n");
			state.Print("└─────────────────────────────┘\n");
		}
		state.Print(data[1]);
	}
};

class ModeListRenderer : public RowRenderer {
public:
	explicit ModeListRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.Print(col_names[i]);
		}
		state.Print(row_sep);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.Print(data[i]);
		}
		state.Print(row_sep);
	}
};

class ModeHtmlRenderer : public RowRenderer {
public:
	explicit ModeHtmlRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		state.Print("<tr>");
		for (idx_t i = 0; i < col_names.size(); i++) {
			state.Print("<th>");
			output_html_string(col_names[i]);
			state.Print("</th>\n");
		}
		state.Print("</tr>\n");
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		state.Print("<tr>");
		for (idx_t i = 0; i < data.size(); i++) {
			state.Print("<td>");
			output_html_string(data[i]);
			state.Print("</td>\n");
		}
		state.Print("</tr>\n");
	}

	/*
	** Output the given string with characters that are special to
	** HTML escaped.
	*/
	void output_html_string(const string &z) {
		string escaped;
		for (auto c : z) {
			switch (c) {
			case '<':
				escaped += "&lt;";
				break;
			case '&':
				escaped += "&amp;";
				break;
			case '>':
				escaped += "&gt;";
				break;
			case '\"':
				escaped += "&quot;";
				break;
			case '\'':
				escaped += "&#39;";
				break;
			default:
				escaped += c;
			}
		}
		state.Print(escaped);
	}
};

class ModeTclRenderer : public RowRenderer {
public:
	explicit ModeTclRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.OutputCString(col_names[i].c_str());
		}
		state.Print(row_sep);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.OutputCString(data[i].c_str());
		}
		state.Print(row_sep);
	}
};

class ModeCsvRenderer : public RowRenderer {
public:
	explicit ModeCsvRenderer(ShellState &state) : RowRenderer(state) {
	}

	void Render(RowResult &result) override {
		state.SetBinaryMode();
		RowRenderer::Render(result);
		state.SetTextMode();
	}
	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			state.OutputCSV(col_names[i].c_str(), i < col_names.size() - 1);
		}
		state.Print(row_sep);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		for (idx_t i = 0; i < data.size(); i++) {
			state.OutputCSV(data[i].c_str(), i < data.size() - 1);
		}
		state.Print(row_sep);
	}
};

class ModeAsciiRenderer : public RowRenderer {
public:
	explicit ModeAsciiRenderer(ShellState &state) : RowRenderer(state) {
		col_sep = "\n";
		row_sep = "\n";
	}

	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.Print(col_names[i]);
		}
		state.Print(row_sep);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.Print(data[i]);
		}
		state.Print(row_sep);
	}
};

class ModeQuoteRenderer : public RowRenderer {
public:
	explicit ModeQuoteRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(RowResult &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			state.OutputQuotedString(col_names[i].c_str());
		}
		state.Print(row_sep);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		auto &types = result.types;
		auto &is_null = result.is_null;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				state.Print(col_sep);
			}
			if (types[i].IsNumeric() || is_null[i]) {
				state.Print(data[i]);
			} else {
				state.OutputQuotedString(data[i].c_str());
			}
		}
		state.Print(row_sep);
	}

	string NullValue() override {
		return "NULL";
	}
};

class ModeJsonRenderer : public RowRenderer {
public:
	explicit ModeJsonRenderer(ShellState &state, bool json_array) : RowRenderer(state), json_array(json_array) {
	}

	void Render(RowResult &result) override {
		if (first_row) {
			if (json_array) {
				// wrap all JSON objects in an array
				state.Print("[");
			}
			state.Print("{");
			first_row = false;
		} else {
			if (json_array) {
				// wrap all JSON objects in an array
				state.Print(",");
			}
			state.Print("\n{");
		}
		RenderRow(result);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = result.is_null;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				state.Print(",");
			}
			state.OutputJSONString(col_names[i].c_str(), -1);
			state.Print(":");
			if (is_null[i]) {
				state.Print(data[i]);
			} else if (types[i].id() == duckdb::LogicalTypeId::FLOAT ||
			           types[i].id() == duckdb::LogicalTypeId::DOUBLE) {
				if (duckdb::StringUtil::Equals(data[i], "inf")) {
					state.Print("1e999");
				} else if (duckdb::StringUtil::Equals(data[i], "-inf")) {
					state.Print("-1e999");
				} else if (duckdb::StringUtil::Equals(data[i], "nan")) {
					state.Print("null");
				} else if (duckdb::StringUtil::Equals(data[i], "-nan")) {
					state.Print("null");
				} else {
					state.Print(data[i]);
				}
			} else if (types[i].IsNumeric() || types[i].IsJSONType()) {
				state.Print(data[i]);
			} else {
				state.OutputJSONString(data[i].c_str(), data[i].size());
			}
		}
		state.Print("}");
	}

	void RenderFooter(RowResult &result) override {
		if (json_array) {
			state.Print("]\n");
		} else {
			state.Print("\n");
		}
	}

	string NullValue() override {
		return "null";
	}

	bool json_array;
};

class ModeInsertRenderer : public RowRenderer {
public:
	explicit ModeInsertRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = result.is_null;

		state.Print("INSERT INTO ");
		state.Print(state.zDestTable);
		if (show_header) {
			state.Print("(");
			for (idx_t i = 0; i < col_names.size(); i++) {
				if (i > 0) {
					state.Print(",");
				}
				state.PrintOptionallyQuotedIdentifier(col_names[i].c_str());
			}
			state.Print(")");
		}
		for (idx_t i = 0; i < data.size(); i++) {
			state.Print(i > 0 ? "," : " VALUES(");
			if (is_null[i]) {
				state.Print("NULL");
			} else if (types[i].IsNumeric()) {
				state.Print(data[i]);
			} else if (state.ShellHasFlag(ShellFlags::SHFLG_Newlines)) {
				state.OutputQuotedString(data[i].c_str());
			} else {
				state.OutputQuotedEscapedString(data[i].c_str());
			}
		}
		state.Print(");\n");
	}
};

class ModeSemiRenderer : public RowRenderer {
public:
	explicit ModeSemiRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(RowResult &result) override {
		/* .schema and .fullschema output */
		state.PrintSchemaLine(result.data[0].c_str(), "\n");
	}
};

class ModePrettyRenderer : public RowRenderer {
public:
	explicit ModePrettyRenderer(ShellState &state) : RowRenderer(state) {
	}

	static bool IsSpace(char c) {
		return duckdb::StringUtil::CharacterIsSpace(c);
	}

	void RenderRow(RowResult &result) override {
		auto &data = result.data;
		/* .schema and .fullschema with --indent */
		if (data.size() != 1) {
			throw std::runtime_error("row must have exactly one value for pretty rendering");
		}
		int j;
		int nParen = 0;
		char cEnd = 0;
		char c;
		int nLine = 0;
		if (duckdb::StringUtil::StartsWith(data[0], "CREATE VIEW") ||
		    duckdb::StringUtil::StartsWith(data[0], "CREATE TRIG")) {
			state.Print(data[0]);
			state.Print(";\n");
			return;
		}
		auto zStr = data[0];
		auto z = (char *)zStr.data();
		j = 0;
		idx_t i;
		for (i = 0; IsSpace(z[i]); i++) {
		}
		for (; (c = z[i]) != 0; i++) {
			if (IsSpace(c)) {
				if (z[j - 1] == '\r') {
					z[j - 1] = '\n';
				}
				if (IsSpace(z[j - 1]) || z[j - 1] == '(') {
					continue;
				}
			} else if ((c == '(' || c == ')') && j > 0 && IsSpace(z[j - 1])) {
				j--;
			}
			z[j++] = c;
		}
		while (j > 0 && IsSpace(z[j - 1])) {
			j--;
		}
		z[j] = 0;
		if (state.StringLength(z) >= 79) {
			for (i = j = 0; (c = z[i]) != 0; i++) { /* Copy from z[i] back to z[j] */
				if (c == cEnd) {
					cEnd = 0;
				} else if (c == '"' || c == '\'' || c == '`') {
					cEnd = c;
				} else if (c == '[') {
					cEnd = ']';
				} else if (c == '-' && z[i + 1] == '-') {
					cEnd = '\n';
				} else if (c == '(') {
					nParen++;
				} else if (c == ')') {
					nParen--;
					if (nLine > 0 && nParen == 0 && j > 0) {
						state.PrintSchemaLineN(z, j, "\n");
						j = 0;
					}
				}
				z[j++] = c;
				if (nParen == 1 && cEnd == 0 && (c == '(' || c == '\n' || (c == ',' && !wsToEol(z + i + 1)))) {
					if (c == '\n')
						j--;
					state.PrintSchemaLineN(z, j, "\n  ");
					j = 0;
					nLine++;
					while (IsSpace(z[i + 1])) {
						i++;
					}
				}
			}
			z[j] = 0;
		}
		state.PrintSchemaLine(z, ";\n");
	}

	/*
	** Return true if string z[] has nothing but whitespace and comments to the
	** end of the first line.
	*/
	static bool wsToEol(const char *z) {
		int i;
		for (i = 0; z[i]; i++) {
			if (z[i] == '\n') {
				return true;
			}
			if (IsSpace(z[i])) {
				continue;
			}
			if (z[i] == '-' && z[i + 1] == '-') {
				return true;
			}
			return false;
		}
		return true;
	}
};

unique_ptr<RowRenderer> ShellState::GetRowRenderer() {
	return GetRowRenderer(cMode);
}

unique_ptr<RowRenderer> ShellState::GetRowRenderer(RenderMode mode) {
	switch (mode) {
	case RenderMode::LINE:
		return unique_ptr<RowRenderer>(new ModeLineRenderer(*this));
	case RenderMode::EXPLAIN:
		return unique_ptr<RowRenderer>(new ModeExplainRenderer(*this));
	case RenderMode::LIST:
		return unique_ptr<RowRenderer>(new ModeListRenderer(*this));
	case RenderMode::HTML:
		return unique_ptr<RowRenderer>(new ModeHtmlRenderer(*this));
	case RenderMode::TCL:
		return unique_ptr<RowRenderer>(new ModeTclRenderer(*this));
	case RenderMode::CSV:
		return unique_ptr<RowRenderer>(new ModeCsvRenderer(*this));
	case RenderMode::ASCII:
		return unique_ptr<RowRenderer>(new ModeAsciiRenderer(*this));
	case RenderMode::QUOTE:
		return unique_ptr<RowRenderer>(new ModeQuoteRenderer(*this));
	case RenderMode::JSON:
		return unique_ptr<RowRenderer>(new ModeJsonRenderer(*this, true));
	case RenderMode::JSONLINES:
		return unique_ptr<RowRenderer>(new ModeJsonRenderer(*this, false));
	case RenderMode::INSERT:
		return unique_ptr<RowRenderer>(new ModeInsertRenderer(*this));
	case RenderMode::SEMI:
		return unique_ptr<RowRenderer>(new ModeSemiRenderer(*this));
	case RenderMode::PRETTY:
		return unique_ptr<RowRenderer>(new ModePrettyRenderer(*this));
	default:
		throw std::runtime_error("Unsupported mode for GetRowRenderer");
	}
}

} // namespace duckdb_shell
