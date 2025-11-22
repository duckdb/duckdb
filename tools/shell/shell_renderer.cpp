#include "shell_renderer.hpp"

#include "shell_state.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "shell_highlight.hpp"
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

void ShellRenderer::RenderHeader(ResultMetadata &result) {
}

void ShellRenderer::RenderRow(ResultMetadata &result, RowData &row) {
}

void ShellRenderer::RenderFooter(ResultMetadata &result) {
}

string ShellRenderer::NullValue() {
	return state.nullValue;
}

void ShellRenderer::Analyze(RenderingQueryResult &result) {
}

// RenderingResultIterator is an iterator that EITHER
// (1) iterates over a query result
// (2) iterates over a materialized result
struct RenderingResultIterator {
public:
	explicit RenderingResultIterator(optional_ptr<RenderingQueryResult> result_p) : result(result_p) {
		if (!result) {
			return;
		}
		auto &query_result = result->result;
		auto nCol = query_result.ColumnCount();
		row_data.data.resize(nCol, string());
		row_data.is_null.resize(nCol, false);
		row_data.row_index = 0;
		if (!result->is_converted) {
			result_iterator = query_result.begin();
		}
		AssignData();
	}

	optional_ptr<RenderingQueryResult> result;
	duckdb::QueryResult::iterator result_iterator;
	RowData row_data;

public:
	void AssignData() {
		auto &query_result = result->result;
		auto nCol = query_result.ColumnCount();
		if (!result->is_converted) {
			// result is not converted - read from query result
			if (result_iterator == query_result.end()) {
				// exhausted
				result = nullptr;
				return;
			}
			auto &row = *result_iterator;
			// convert the result
			for (idx_t c = 0; c < nCol; c++) {
				if (row.IsNull(c)) {
					row_data.is_null[c] = true;
					row_data.data[c] = result->renderer.NullValue();
				} else {
					row_data.is_null[c] = false;
					row_data.data[c] = row.GetValue<string>(c);
				}
			}
			return;
		}
		// result is already converted - just copy it over
		if (row_data.row_index >= result->data.size()) {
			result = nullptr;
			return;
		}
		row_data.data = std::move(result->data[row_data.row_index]);
	}

	void Next() {
		if (!result) {
			return;
		}
		// iterate to next position
		row_data.row_index++;
		if (!result->is_converted) {
			++result_iterator;
		}
		// read data from this position
		AssignData();
	}

	RenderingResultIterator &operator++() {
		Next();
		return *this;
	}
	bool operator!=(const RenderingResultIterator &other) const {
		return result != other.result;
	}
	RowData &operator*() {
		return row_data;
	}
};

RenderingResultIterator RenderingQueryResult::begin() {
	return RenderingResultIterator(*this);
}

RenderingResultIterator RenderingQueryResult::end() {
	return RenderingResultIterator(nullptr);
}

SuccessState ShellState::RenderQueryResult(ShellRenderer &renderer, duckdb::QueryResult &query_result) {
	RenderingQueryResult result(query_result, renderer);

	renderer.Analyze(result);
	return renderer.RenderQueryResult(*this, result);
}

SuccessState ShellRenderer::RenderQueryResult(ShellState &state, RenderingQueryResult &result) {
	RenderHeader(result.metadata);
	for (auto &row_data : result) {
		if (state.seenInterrupt) {
			state.PrintF("Interrupt\n");
			return SuccessState::FAILURE;
		}
		RenderRow(result.metadata, row_data);
	}
	RenderFooter(result.metadata);
	return SuccessState::SUCCESS;
}

//===--------------------------------------------------------------------===//
// Result Metadata
//===--------------------------------------------------------------------===//
string GetTypeName(duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case duckdb::LogicalTypeId::TINYINT:
		return "TINYINT";
	case duckdb::LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case duckdb::LogicalTypeId::INTEGER:
		return "INTEGER";
	case duckdb::LogicalTypeId::BIGINT:
		return "BIGINT";
	case duckdb::LogicalTypeId::FLOAT:
		return "FLOAT";
	case duckdb::LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case duckdb::LogicalTypeId::DECIMAL:
		return "DECIMAL";
	case duckdb::LogicalTypeId::DATE:
		return "DATE";
	case duckdb::LogicalTypeId::TIME:
		return "TIME";
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
		return "TIMESTAMP";
	case duckdb::LogicalTypeId::VARCHAR:
		return "VARCHAR";
	case duckdb::LogicalTypeId::LIST:
		return "LIST";
	case duckdb::LogicalTypeId::MAP:
		return "MAP";
	case duckdb::LogicalTypeId::STRUCT:
		return "STRUCT";
	case duckdb::LogicalTypeId::BLOB:
		return "BLOB";
	default:
		return "NULL";
	}
}

ResultMetadata::ResultMetadata(duckdb::QueryResult &result) {
	// initialize the result and the column names
	idx_t nCol = result.ColumnCount();
	column_names.reserve(nCol);
	types.reserve(nCol);
	for (idx_t c = 0; c < nCol; c++) {
		column_names.push_back(result.names[c]);
		types.push_back(result.types[c]);
		type_names.push_back(GetTypeName(result.types[c]));
	}
}

//===--------------------------------------------------------------------===//
// Column Renderers
//===--------------------------------------------------------------------===//
ColumnRenderer::ColumnRenderer(ShellState &state) : ShellRenderer(state) {
}

string ColumnRenderer::ConvertValue(const char *value) {
	return value ? value : state.nullValue;
}

void ColumnRenderer::RenderAlignedValue(ResultMetadata &result, idx_t c) {
	auto &header_value = result.column_names[c];
	idx_t w = column_width[c];
	idx_t n = state.RenderLength(header_value);
	idx_t lspace = (w - n) / 2;
	idx_t rspace = (w - n + 1) / 2;
	state.Print(string(lspace, ' '));
	state.Print(header_value);
	state.Print(string(rspace, ' '));
}

void ColumnRenderer::Analyze(RenderingQueryResult &result) {
	auto &state = ShellState::Get();
	for (auto &column_name : result.metadata.column_names) {
		column_name = ConvertValue(column_name.c_str());
	}
	auto &query_result = result.result;
	// materialize the query result
	for (auto &row : query_result) {
		if (state.seenInterrupt) {
			state.PrintF("Interrupt\n");
			return;
		}
		vector<string> row_data;
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			auto str_val = row.GetValue<string>(c);
			row_data.push_back(ConvertValue(str_val.c_str()));
		}
		result.data.push_back(std::move(row_data));
	}
	result.is_converted = true;

	// compute the column widths
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		int w = c < state.colWidth.size() ? state.colWidth[c] : 0;
		if (w < 0) {
			right_align.push_back(true);
			w = -w;
		} else {
			right_align.push_back(false);
		}
		idx_t render_width = static_cast<idx_t>(w);
		idx_t column_name_width = state.RenderLength(result.metadata.column_names[c]);
		if (column_name_width > render_width) {
			render_width = column_name_width;
		}
		column_width.push_back(render_width);
	}
	for (auto &row : result.data) {
		for (idx_t column_idx = 0; column_idx < row.size(); column_idx++) {
			idx_t width = state.RenderLength(row[column_idx]);
			if (width > column_width[column_idx]) {
				column_width[column_idx] = width;
			}
		}
	}
}

void ColumnRenderer::RenderRow(ResultMetadata &result, RowData &row) {
	auto &state = ShellState::Get();
	auto colSep = GetColumnSeparator();
	auto rowSep = GetRowSeparator();
	auto row_start = GetRowStart();
	if (row_start) {
		state.Print(row_start);
	}
	for (idx_t c = 0; c < row.data.size(); c++) {
		if (c > 0) {
			state.Print(colSep);
		}
		idx_t w = column_width[c];
		bool is_right_aligned = right_align[c];
		state.UTF8WidthPrint(w, row.data[c], is_right_aligned);
	}
	state.Print(rowSep);
}

class ModeColumnRenderer : public ColumnRenderer {
public:
	explicit ModeColumnRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto column_count = result.ColumnCount();
		for (idx_t c = 0; c < column_count; c++) {
			state.UTF8WidthPrint(column_width[c], result.column_names[c], right_align[c]);
			state.Print(c == column_count - 1 ? "\n" : "  ");
		}
		for (idx_t i = 0; i < column_count; i++) {
			state.PrintDashes(column_width[i]);
			state.Print(i == column_count - 1 ? "\n" : "  ");
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

	void RenderHeader(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		state.PrintRowSeparator(column_count, "+", column_width);
		state.Print("| ");
		for (idx_t c = 0; c < column_count; c++) {
			RenderAlignedValue(result, c);
			state.Print(c == column_count - 1 ? " |\n" : " | ");
		}
		state.PrintRowSeparator(column_count, "+", column_width);
	}

	void RenderFooter(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		state.PrintRowSeparator(column_count, "+", column_width);
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

	void RenderHeader(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		state.Print(GetRowStart());
		for (idx_t c = 0; c < column_count; c++) {
			if (c > 0) {
				state.Print(GetColumnSeparator());
			}
			RenderAlignedValue(result, c);
		}
		state.Print(GetRowSeparator());
		state.PrintMarkdownSeparator(column_count, "|", result.types, column_width);
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

	void RenderHeader(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		print_box_row_separator(column_count, BOX_23, BOX_234, BOX_34, column_width);
		state.Print(BOX_13 " ");
		for (idx_t c = 0; c < column_count; c++) {
			RenderAlignedValue(result, c);
			state.Print(c == column_count - 1 ? " " BOX_13 "\n" : " " BOX_13 " ");
		}
		print_box_row_separator(column_count, BOX_123, BOX_1234, BOX_134, column_width);
	}

	void RenderFooter(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		print_box_row_separator(column_count, BOX_12, BOX_124, BOX_14, column_width);
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

	void RenderHeader(ResultMetadata &result) override {
		auto column_count = result.ColumnCount();

		state.Print("\\begin{tabular}{|");
		for (idx_t i = 0; i < column_count; i++) {
			if (state.ColumnTypeIsInteger(result.type_names[i].c_str())) {
				state.Print("r");
			} else {
				state.Print("l");
			}
		}
		state.Print("|}\n");
		state.Print("\\hline\n");
		for (idx_t i = 0; i < column_count; i++) {
			RenderAlignedValue(result, i);
			state.Print(i == column_count - 1 ? GetRowSeparator() : GetColumnSeparator());
		}
		state.Print("\\hline\n");
	}

	void RenderFooter(ResultMetadata &) override {
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

//===--------------------------------------------------------------------===//
// Row Renderers
//===--------------------------------------------------------------------===//
RowRenderer::RowRenderer(ShellState &state) : ShellRenderer(state) {
}

void RowRenderer::RenderHeader(ResultMetadata &result) {
}

class ModeLineRenderer : public RowRenderer {
public:
	explicit ModeLineRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(ResultMetadata &result) override {
		auto &col_names = result.column_names;
		// determine the render width by going over the column names
		header_width = 5;
		for (idx_t i = 0; i < col_names.size(); i++) {
			auto len = col_names[i].size();
			if (len > header_width) {
				header_width = len;
			}
		}
	}

	void RenderRow(ResultMetadata &result, RowData &row) override {
		if (row.row_index > 0) {
			state.Print(state.rowSeparator);
		}
		auto &data = row.data;
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < data.size(); i++) {
			idx_t space_count = header_width - col_names[i].size();
			if (space_count > 0) {
				state.Print(string(space_count, ' '));
			}
			state.Print(col_names[i]);
			state.Print(" = ");
			state.Print(data[i]);
			state.Print(state.rowSeparator);
		}
	}

	idx_t header_width = 0;
};

class ModeExplainRenderer : public RowRenderer {
public:
	explicit ModeExplainRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

	void RenderHeader(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

	void RenderHeader(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

	void RenderHeader(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

	void RenderHeader(ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			state.OutputCSV(col_names[i].c_str(), i < col_names.size() - 1);
		}
		state.Print(row_sep);
	}

	void RenderRow(ResultMetadata &result, RowData &row) override {
		state.SetBinaryMode();
		auto &data = row.data;
		for (idx_t i = 0; i < data.size(); i++) {
			state.OutputCSV(data[i].c_str(), i < data.size() - 1);
		}
		state.Print(row_sep);
		state.SetTextMode();
	}
};

class ModeAsciiRenderer : public RowRenderer {
public:
	explicit ModeAsciiRenderer(ShellState &state) : RowRenderer(state) {
		col_sep = "\n";
		row_sep = "\n";
	}

	void RenderHeader(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

	void RenderHeader(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		auto &types = result.types;
		auto &is_null = row.is_null;
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

	void RenderHeader(ResultMetadata &result) override {
		if (json_array) {
			// wrap all JSON objects in an array
			state.Print("[");
		}
		state.Print("{");
	}

	void RenderRow(ResultMetadata &result, RowData &row) override {
		if (row.row_index > 0) {
			if (json_array) {
				// wrap all JSON objects in an array
				state.Print(",");
			}
			state.Print("\n{");
		}
		auto &data = row.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = row.is_null;
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

	void RenderFooter(ResultMetadata &result) override {
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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = row.is_null;

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

	void RenderRow(ResultMetadata &result, RowData &row) override {
		/* .schema and .fullschema output */
		state.PrintSchemaLine(row.data[0].c_str(), "\n");
	}
};

class ModePrettyRenderer : public RowRenderer {
public:
	explicit ModePrettyRenderer(ShellState &state) : RowRenderer(state) {
	}

	static bool IsSpace(char c) {
		return duckdb::StringUtil::CharacterIsSpace(c);
	}

	void RenderRow(ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
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

//===--------------------------------------------------------------------===//
// DuckBox Renderer
//===--------------------------------------------------------------------===//
class DuckBoxRenderer : public duckdb::BaseResultRenderer {
public:
	DuckBoxRenderer(ShellState &state, bool highlight)
	    : shell_highlight(state), output(PrintOutput::STDOUT), highlight(highlight) {
	}

	void RenderLayout(const string &text) override {
		PrintText(text, HighlightElementType::LAYOUT);
	}

	void RenderColumnName(const string &text) override {
		PrintText(text, HighlightElementType::COLUMN_NAME);
	}

	void RenderType(const string &text) override {
		PrintText(text, HighlightElementType::COLUMN_TYPE);
	}

	void RenderValue(const string &text, const duckdb::LogicalType &type) override {
		if (type.IsNumeric()) {
			PrintText(text, HighlightElementType::NUMERIC_VALUE);
		} else if (type.IsTemporal()) {
			PrintText(text, HighlightElementType::TEMPORAL_VALUE);
		} else {
			PrintText(text, HighlightElementType::STRING_VALUE);
		}
	}

	void RenderStringLiteral(const string &text, const duckdb::LogicalType &type) override {
		PrintText(text, HighlightElementType::STRING_CONSTANT);
	}

	void RenderNull(const string &text, const duckdb::LogicalType &type) override {
		PrintText(text, HighlightElementType::NULL_VALUE);
	}

	void RenderFooter(const string &text) override {
		PrintText(text, HighlightElementType::FOOTER);
	}

	void PrintText(const string &text, HighlightElementType element_type) {
		if (shell_highlight.state.seenInterrupt) {
			return;
		}
		if (highlight) {
			shell_highlight.PrintText(text, output, element_type);
		} else {
			shell_highlight.state.Print(text);
		}
	}

private:
	ShellHighlight shell_highlight;
	PrintOutput output;
	bool highlight = true;
};

ModeDuckBoxRenderer::ModeDuckBoxRenderer(ShellState &state) : ShellRenderer(state) {
}

SuccessState ModeDuckBoxRenderer::RenderQueryResult(ShellState &state, RenderingQueryResult &result) {
	DuckBoxRenderer result_renderer(state, state.HighlightResults());
	try {
		duckdb::BoxRendererConfig config;
		config.max_rows = state.max_rows;
		config.max_width = state.max_width;
		if (config.max_width == 0) {
			// if max_width is set to 0 (auto) - set it to infinite if we are writing to a file
			if (!state.outfile.empty() && state.outfile[0] != '|') {
				config.max_rows = (size_t)-1;
				config.max_width = (size_t)-1;
			}
			if (!state.stdout_is_console) {
				config.max_width = (size_t)-1;
			}
		}
		LargeNumberRendering large_rendering = state.large_number_rendering;
		if (large_rendering == LargeNumberRendering::DEFAULT) {
			large_rendering = state.stdout_is_console ? LargeNumberRendering::FOOTER : LargeNumberRendering::NONE;
		}
		config.null_value = state.nullValue;
		if (state.columns) {
			config.render_mode = duckdb::RenderMode::COLUMNS;
		}
		config.decimal_separator = state.decimal_separator;
		config.thousand_separator = state.thousand_separator;
		config.large_number_rendering = static_cast<duckdb::LargeNumberRendering>(static_cast<int>(large_rendering));
		duckdb::BoxRenderer renderer(config);
		auto &query_result = result.result;
		auto &materialized = query_result.Cast<duckdb::MaterializedQueryResult>();
		auto &con = *state.conn;
		renderer.Render(*con.context, result.metadata.column_names, materialized.Collection(), result_renderer);
		return SuccessState::SUCCESS;
	} catch (std::exception &ex) {
		string error_str = duckdb::ErrorData(ex).Message() + "\n";
		result_renderer.RenderLayout(error_str);
		return SuccessState::FAILURE;
	}
}

//===--------------------------------------------------------------------===//
// Get Renderer
//===--------------------------------------------------------------------===//
unique_ptr<ShellRenderer> ShellState::GetRenderer() {
	return GetRenderer(cMode);
}

unique_ptr<ShellRenderer> ShellState::GetRenderer(RenderMode mode) {
	switch (mode) {
	case RenderMode::LINE:
		return make_uniq<ModeLineRenderer>(*this);
	case RenderMode::EXPLAIN:
		return make_uniq<ModeExplainRenderer>(*this);
	case RenderMode::LIST:
		return make_uniq<ModeListRenderer>(*this);
	case RenderMode::HTML:
		return make_uniq<ModeHtmlRenderer>(*this);
	case RenderMode::TCL:
		return make_uniq<ModeTclRenderer>(*this);
	case RenderMode::CSV:
		return make_uniq<ModeCsvRenderer>(*this);
	case RenderMode::ASCII:
		return make_uniq<ModeAsciiRenderer>(*this);
	case RenderMode::QUOTE:
		return make_uniq<ModeQuoteRenderer>(*this);
	case RenderMode::JSON:
		return make_uniq<ModeJsonRenderer>(*this, true);
	case RenderMode::JSONLINES:
		return make_uniq<ModeJsonRenderer>(*this, false);
	case RenderMode::INSERT:
		return make_uniq<ModeInsertRenderer>(*this);
	case RenderMode::SEMI:
		return make_uniq<ModeSemiRenderer>(*this);
	case RenderMode::PRETTY:
		return make_uniq<ModePrettyRenderer>(*this);
	case RenderMode::COLUMN:
		return make_uniq<ModeColumnRenderer>(*this);
	case RenderMode::TABLE:
		return make_uniq<ModeTableRenderer>(*this);
	case RenderMode::MARKDOWN:
		return make_uniq<ModeMarkdownRenderer>(*this);
	case RenderMode::BOX:
		return make_uniq<ModeBoxRenderer>(*this);
	case RenderMode::LATEX:
		return make_uniq<ModeLatexRenderer>(*this);
	case RenderMode::DUCKBOX:
		return make_uniq<ModeDuckBoxRenderer>(*this);
	default:
		throw std::runtime_error("Unsupported mode for GetRenderer");
	}
}

} // namespace duckdb_shell
