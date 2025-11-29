#include "shell_renderer.hpp"

#include "shell_state.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "shell_highlight.hpp"
#include <stdexcept>
#include <cstring>

namespace duckdb_shell {

//===--------------------------------------------------------------------===//
// Print Stream
//===--------------------------------------------------------------------===//
PrintStream::PrintStream(ShellState &state) : state(state) {
}

void PrintStream::RenderAlignedValue(const string &str, idx_t width, TextAlignment alignment) {
	idx_t w = width;
	idx_t n = state.RenderLength(str);
	idx_t space_count = w < n ? 0 : w - n;
	if (alignment == TextAlignment::LEFT) {
		Print(str);
		Print(string(space_count, ' '));
		return;
	}
	if (alignment == TextAlignment::RIGHT) {
		Print(string(space_count, ' '));
		Print(str);
		return;
	}
	idx_t lspace = space_count / 2;
	idx_t rspace = (space_count + 1) / 2;
	Print(string(lspace, ' '));
	Print(str);
	Print(string(rspace, ' '));
}

void PrintStream::PrintDashes(idx_t N) {
	Print(string(N, '-'));
}

void PrintStream::OutputQuotedIdentifier(const string &str) {
	Print(StringUtil::Format("%s", SQLIdentifier(str)));
}

void PrintStream::OutputQuotedString(const string &str) {
	Print(StringUtil::Format("%s", SQLString(str)));
}

//===--------------------------------------------------------------------===//
// ShellRenderer
//===--------------------------------------------------------------------===//
ShellRenderer::ShellRenderer(ShellState &state)
    : state(state), show_header(state.showHeader), col_sep(state.colSeparator), row_sep(state.rowSeparator) {
}

void ShellRenderer::RenderHeader(PrintStream &out, ResultMetadata &result) {
}

void ShellRenderer::RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) {
}

void ShellRenderer::RenderFooter(PrintStream &out, ResultMetadata &result) {
}

string ShellRenderer::NullValue() {
	return state.nullValue;
}

string ShellRenderer::ConvertValue(const char *value) {
	return value ? value : state.nullValue;
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
		if (!result->exhausted_result && row_data.row_index >= result->data.size()) {
			result->TryConvertChunk();
		}
		AssignData();
	}

	optional_ptr<RenderingQueryResult> result;
	RowData row_data;

public:
	void AssignData() {
		auto &query_result = result->result;
		if (row_data.row_index >= result->data.size()) {
			result = nullptr;
			return;
		}
		// read from the materialized rows
		row_data.data = std::move(result->data[row_data.row_index]);
	}

	void Next() {
		if (!result) {
			return;
		}
		// iterate to next position
		row_data.row_index++;
		if (!result->exhausted_result && row_data.row_index >= result->data.size()) {
			// convert the next chunk (if we have any)
			result->TryConvertChunk();
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
	PrintStream print_stream(*this);
	return renderer.RenderQueryResult(print_stream, *this, result);
}

SuccessState ShellRenderer::RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) {
	RenderHeader(out, result.metadata);
	for (auto &row_data : result) {
		if (state.seenInterrupt) {
			state.PrintF("Interrupt\n");
			return SuccessState::FAILURE;
		}
		RenderRow(out, result.metadata, row_data);
	}
	RenderFooter(out, result.metadata);
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

bool RenderingQueryResult::TryConvertChunk() {
	if (exhausted_result) {
		return false;
	}
	auto chunk = result.Fetch();
	if (!chunk) {
		exhausted_result = true;
		return false;
	}
	for (idx_t r = 0; r < chunk->size(); r++) {
		vector<string> row_data;
		for (idx_t c = 0; c < result.ColumnCount(); c++) {
			auto str_val = chunk->data[c].GetValue(r).GetValue<string>();
			row_data.push_back(renderer.ConvertValue(str_val.c_str()));
		}
		data.push_back(std::move(row_data));
	}
	return true;
}

void ColumnRenderer::Analyze(RenderingQueryResult &result) {
	auto &state = ShellState::Get();
	for (auto &column_name : result.metadata.column_names) {
		column_name = ConvertValue(column_name.c_str());
	}
	// materialize the query result
	while (result.TryConvertChunk()) {
		if (state.seenInterrupt) {
			state.PrintF("Interrupt\n");
			return;
		}
	}

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

bool ColumnRenderer::ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) {
	if (global_mode == PagerMode::PAGER_ON) {
		return true;
	}
	if (result.data.size() >= state.pager_min_rows) {
		// rows exceed min rows
		return true;
	}
	idx_t max_render_width = state.GetMaxRenderWidth();
	idx_t total_width = 0;
	for (auto w : column_width) {
		total_width += w + 2;
	}
	if (total_width > max_render_width) {
		return true;
	}
	return false;
}

void ColumnRenderer::RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) {
	auto colSep = GetColumnSeparator();
	auto rowSep = GetRowSeparator();
	auto row_start = GetRowStart();
	if (row_start) {
		out.Print(row_start);
	}
	for (idx_t c = 0; c < row.data.size(); c++) {
		if (c > 0) {
			out.Print(colSep);
		}
		TextAlignment alignment = right_align[c] ? TextAlignment::RIGHT : TextAlignment::LEFT;
		out.RenderAlignedValue(row.data[c], column_width[c], alignment);
	}
	out.Print(rowSep);
}

class ModeColumnRenderer : public ColumnRenderer {
public:
	explicit ModeColumnRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto column_count = result.ColumnCount();
		for (idx_t c = 0; c < column_count; c++) {
			TextAlignment alignment = right_align[c] ? TextAlignment::RIGHT : TextAlignment::LEFT;
			out.RenderAlignedValue(result.column_names[c], column_width[c], alignment);
			out.Print(c == column_count - 1 ? "\n" : "  ");
		}
		for (idx_t i = 0; i < column_count; i++) {
			out.PrintDashes(column_width[i]);
			out.Print(i == column_count - 1 ? "\n" : "  ");
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

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		PrintRowSeparator(out, column_count, "+", column_width);
		out.Print("| ");
		for (idx_t c = 0; c < column_count; c++) {
			out.RenderAlignedValue(result.column_names[c], column_width[c]);
			out.Print(c == column_count - 1 ? " |\n" : " | ");
		}
		PrintRowSeparator(out, column_count, "+", column_width);
	}

	void RenderFooter(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		PrintRowSeparator(out, column_count, "+", column_width);
	}

	void PrintRowSeparator(PrintStream &out, idx_t nArg, const char *zSep, const vector<idx_t> &actualWidth) {
		if (nArg > 0) {
			out.Print(zSep);
			out.PrintDashes(actualWidth[0] + 2);
			for (idx_t i = 1; i < nArg; i++) {
				out.Print(zSep);
				out.PrintDashes(actualWidth[i] + 2);
			}
			out.Print(zSep);
		}
		out.Print("\n");
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

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		out.Print(GetRowStart());
		for (idx_t c = 0; c < column_count; c++) {
			if (c > 0) {
				out.Print(GetColumnSeparator());
			}
			out.RenderAlignedValue(result.column_names[c], column_width[c]);
		}
		out.Print(GetRowSeparator());
		PrintMarkdownSeparator(out, column_count, "|", result.types, column_width);
	}

	void PrintMarkdownSeparator(PrintStream &out, idx_t nArg, const char *zSep,
	                            const vector<duckdb::LogicalType> &colTypes, const vector<idx_t> &actualWidth) {
		if (nArg > 0) {
			for (idx_t i = 0; i < nArg; i++) {
				out.Print(zSep);
				if (colTypes[i].IsNumeric()) {
					// right-align numerics in tables
					out.PrintDashes(actualWidth[i] + 1);
					out.Print(":");
				} else {
					out.PrintDashes(actualWidth[i] + 2);
				}
			}
			out.Print(zSep);
		}
		out.Print("\n");
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

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		PrintBoxRowSeparator(out, column_count, BOX_23, BOX_234, BOX_34, column_width);
		out.Print(BOX_13 " ");
		for (idx_t c = 0; c < column_count; c++) {
			out.RenderAlignedValue(result.column_names[c], column_width[c]);
			out.Print(c == column_count - 1 ? " " BOX_13 "\n" : " " BOX_13 " ");
		}
		PrintBoxRowSeparator(out, column_count, BOX_123, BOX_1234, BOX_134, column_width);
	}

	void RenderFooter(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();
		PrintBoxRowSeparator(out, column_count, BOX_12, BOX_124, BOX_14, column_width);
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
	void PrintBoxLine(PrintStream &out, idx_t N) {
		string box_line;
		for (idx_t i = 0; i < N; i++) {
			box_line += BOX_24;
		}
		out.Print(box_line);
	}

	/*
	** Draw a horizontal separator for a RenderMode::Box table.
	*/
	void PrintBoxRowSeparator(PrintStream &out, int nArg, const char *zSep1, const char *zSep2, const char *zSep3,
	                          const vector<idx_t> &actualWidth) {
		int i;
		if (nArg > 0) {
			out.Print(zSep1);
			PrintBoxLine(out, actualWidth[0] + 2);
			for (i = 1; i < nArg; i++) {
				out.Print(zSep2);
				PrintBoxLine(out, actualWidth[i] + 2);
			}
			out.Print(zSep3);
		}
		out.Print("\n");
	}
};

class ModeLatexRenderer : public ColumnRenderer {
public:
	explicit ModeLatexRenderer(ShellState &state) : ColumnRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		auto column_count = result.ColumnCount();

		out.Print("\\begin{tabular}{|");
		for (idx_t i = 0; i < column_count; i++) {
			if (state.ColumnTypeIsInteger(result.type_names[i].c_str())) {
				out.Print("r");
			} else {
				out.Print("l");
			}
		}
		out.Print("|}\n");
		out.Print("\\hline\n");
		for (idx_t c = 0; c < column_count; c++) {
			out.RenderAlignedValue(result.column_names[c], column_width[c]);
			out.Print(c == column_count - 1 ? GetRowSeparator() : GetColumnSeparator());
		}
		out.Print("\\hline\n");
	}

	void RenderFooter(PrintStream &out, ResultMetadata &) override {
		out.Print("\\hline\n");
		out.Print("\\end{tabular}\n");
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

void RowRenderer::RenderHeader(PrintStream &out, ResultMetadata &result) {
}

struct WidthMeasuringStream : public PrintStream {
public:
	explicit WidthMeasuringStream(ShellState &state) : PrintStream(state) {
	}

	void Print(const string &str) override {
		for (auto c : str) {
			if (c == '\r') {
				continue;
			}
			if (c == '\n') {
				// newline - compute the render width of the current line and reset
				auto render_width = state.RenderLength(output);
				if (render_width > max_width) {
					max_width = render_width;
				}
				output = string();
				continue;
			}
			output += c;
		}
	}
	void SetBinaryMode() override {
	}
	void SetTextMode() override {
	}
	bool SupportsHighlight() override {
		return false;
	}

	string output;
	idx_t max_width = 0;
};

bool RowRenderer::ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) {
	if (global_mode == PagerMode::PAGER_ON) {
		return true;
	}
	// fetch data until we have either fetched more than the pager min, or we have exhausted the data
	while (result.data.size() < state.pager_min_rows && result.TryConvertChunk()) {
	}
	if (result.data.size() >= state.pager_min_rows) {
		// rows exceed min rows
		return true;
	}
	D_ASSERT(result.exhausted_result);
	// figure out how wide the result would be when rendered
	WidthMeasuringStream stream(state);
	// make a copy of the result to avoid consuming it
	auto result_copy = result.data;
	RenderQueryResult(stream, state, result);
	result.data = std::move(result_copy);

	idx_t max_render_width = state.GetMaxRenderWidth();
	if (stream.max_width > max_render_width) {
		return true;
	}
	return false;
}

class ModeLineRenderer : public RowRenderer {
public:
	explicit ModeLineRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
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

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		if (row.row_index > 0) {
			out.Print(state.rowSeparator);
		}
		auto &data = row.data;
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < data.size(); i++) {
			idx_t space_count = header_width - col_names[i].size();
			if (space_count > 0) {
				out.Print(string(space_count, ' '));
			}
			out.Print(col_names[i]);
			out.Print(" = ");
			out.Print(data[i]);
			out.Print(state.rowSeparator);
		}
	}

	idx_t header_width = 0;
};

class ModeExplainRenderer : public RowRenderer {
public:
	explicit ModeExplainRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		if (data.size() != 2) {
			return;
		}
		if (duckdb::StringUtil::Equals(data[0], "logical_plan") || duckdb::StringUtil::Equals(data[0], "logical_opt") ||
		    duckdb::StringUtil::Equals(data[0], "physical_plan")) {
			out.Print("\n┌─────────────────────────────┐\n");
			out.Print("│┌───────────────────────────┐│\n");
			if (duckdb::StringUtil::Equals(data[0], "logical_plan")) {
				out.Print("││ Unoptimized Logical Plan  ││\n");
			} else if (duckdb::StringUtil::Equals(data[0], "logical_opt")) {
				out.Print("││  Optimized Logical Plan   ││\n");
			} else if (duckdb::StringUtil::Equals(data[0], "physical_plan")) {
				out.Print("││       Physical Plan       ││\n");
			}
			out.Print("│└───────────────────────────┘│\n");
			out.Print("└─────────────────────────────┘\n");
		}
		out.Print(data[1]);
	}

	bool RequireMaterializedResult() const override {
		return true;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override {
		if (global_mode == PagerMode::PAGER_ON) {
			return true;
		}
		idx_t row_count = 0;
		for (auto &row : result.data) {
			for (auto c : row[1]) {
				if (c == '\n') {
					row_count++;
				}
			}
		}
		return row_count >= state.pager_min_rows;
	}
};

class ModeListRenderer : public RowRenderer {
public:
	explicit ModeListRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(col_names[i]);
		}
		out.Print(row_sep);
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(data[i]);
		}
		out.Print(row_sep);
	}
};

class ModeHtmlRenderer : public RowRenderer {
public:
	explicit ModeHtmlRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		out.Print("<tr>");
		for (idx_t i = 0; i < col_names.size(); i++) {
			out.Print("<th>");
			OutputHTMLString(out, col_names[i]);
			out.Print("</th>\n");
		}
		out.Print("</tr>\n");
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		out.Print("<tr>");
		for (idx_t i = 0; i < data.size(); i++) {
			out.Print("<td>");
			OutputHTMLString(out, data[i]);
			out.Print("</td>\n");
		}
		out.Print("</tr>\n");
	}

	/*
	** Output the given string with characters that are special to
	** HTML escaped.
	*/
	void OutputHTMLString(PrintStream &out, const string &z) {
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
		out.Print(escaped);
	}
};

class ModeTclRenderer : public RowRenderer {
public:
	explicit ModeTclRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(state.EscapeCString(col_names[i].c_str()));
		}
		out.Print(row_sep);
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(state.EscapeCString(data[i].c_str()));
		}
		out.Print(row_sep);
	}
};

class ModeCsvRenderer : public RowRenderer {
public:
	explicit ModeCsvRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		out.SetBinaryMode();
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			out.Print(EscapeCSV(col_names[i], i < col_names.size() - 1));
		}
		out.Print(row_sep);
		out.SetTextMode();
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		out.SetBinaryMode();
		auto &data = row.data;
		for (idx_t i = 0; i < data.size(); i++) {
			out.Print(EscapeCSV(data[i].c_str(), i < data.size() - 1));
		}
		out.Print(row_sep);
		out.SetTextMode();
	}

	bool CharacterNeedsQuote(unsigned char c) {
		if (c <= 31) {
			// non-printable - needs quote
			return true;
		}
		if (c == '"') {
			return true;
		}
		if (c >= 123) {
			return true;
		}
		return false;
	}

	string EscapeCSV(const string &str, bool print_sep) {
		string result;
		bool needs_quote = false;
		for (idx_t idx = 0; idx < str.size(); idx++) {
			if (CharacterNeedsQuote((unsigned char)str[idx])) {
				needs_quote = true;
				break;
			}
		}
		if (!needs_quote && StringUtil::Contains(str, state.colSeparator)) {
			needs_quote = true;
		}
		if (needs_quote) {
			auto zQuoted = StringUtil::Format("%s", SQLIdentifier(str));
			result += zQuoted;
		} else {
			result += str;
		}
		if (print_sep) {
			result += state.colSeparator;
		}
		return result;
	}
};

class ModeAsciiRenderer : public RowRenderer {
public:
	explicit ModeAsciiRenderer(ShellState &state) : RowRenderer(state) {
		col_sep = "\n";
		row_sep = "\n";
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(col_names[i]);
		}
		out.Print(row_sep);
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.Print(data[i]);
		}
		out.Print(row_sep);
	}
};

class ModeQuoteRenderer : public RowRenderer {
public:
	explicit ModeQuoteRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (!show_header) {
			return;
		}
		auto &col_names = result.column_names;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			out.OutputQuotedString(col_names[i].c_str());
		}
		out.Print(row_sep);
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		auto &types = result.types;
		auto &is_null = row.is_null;
		for (idx_t i = 0; i < data.size(); i++) {
			if (i > 0) {
				out.Print(col_sep);
			}
			if (types[i].IsNumeric() || is_null[i]) {
				out.Print(data[i]);
			} else {
				out.OutputQuotedString(data[i].c_str());
			}
		}
		out.Print(row_sep);
	}

	string NullValue() override {
		return "NULL";
	}
};

class ModeJsonRenderer : public RowRenderer {
public:
	explicit ModeJsonRenderer(ShellState &state, bool json_array) : RowRenderer(state), json_array(json_array) {
	}

	void RenderHeader(PrintStream &out, ResultMetadata &result) override {
		if (json_array) {
			// wrap all JSON objects in an array
			out.Print("[");
		}
		out.Print("{");
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		if (row.row_index > 0) {
			if (json_array) {
				// wrap all JSON objects in an array
				out.Print(",");
			}
			out.Print("\n{");
		}
		auto &data = row.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = row.is_null;
		for (idx_t i = 0; i < col_names.size(); i++) {
			if (i > 0) {
				out.Print(",");
			}
			out.Print(EscapeJSONString(col_names[i]));
			out.Print(":");
			if (is_null[i]) {
				out.Print(data[i]);
			} else if (types[i].id() == duckdb::LogicalTypeId::FLOAT ||
			           types[i].id() == duckdb::LogicalTypeId::DOUBLE) {
				if (duckdb::StringUtil::Equals(data[i], "inf")) {
					out.Print("1e999");
				} else if (duckdb::StringUtil::Equals(data[i], "-inf")) {
					out.Print("-1e999");
				} else if (duckdb::StringUtil::Equals(data[i], "nan")) {
					out.Print("null");
				} else if (duckdb::StringUtil::Equals(data[i], "-nan")) {
					out.Print("null");
				} else {
					out.Print(data[i]);
				}
			} else if (types[i].IsNumeric() || types[i].IsJSONType()) {
				out.Print(data[i]);
			} else {
				out.Print(EscapeJSONString(data[i]));
			}
		}
		out.Print("}");
	}

	string EscapeJSONString(const string &str) {
		string result = "\"";
		for (auto c : str) {
			if (c == '\\' || c == '"') {
				// escape \ and "
				result += "\\";
				result += c;
			} else if (c <= 0x1f) {
				result += "\\";
				if (c == '\b') {
					result += "b";
				} else if (c == '\f') {
					result += "f";
				} else if (c == '\n') {
					result += "n";
				} else if (c == '\r') {
					result += "r";
				} else if (c == '\t') {
					result += "t";
				} else {
					char buf[10];
					snprintf(buf, 10, "u%04x", c);
					result += buf;
				}
			} else {
				result += c;
			}
		}
		result += "\"";
		return result;
	}

	void RenderFooter(PrintStream &out, ResultMetadata &result) override {
		if (json_array) {
			out.Print("]\n");
		} else {
			out.Print("\n");
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

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		auto &data = row.data;
		auto &types = result.types;
		auto &col_names = result.column_names;
		auto &is_null = row.is_null;

		out.Print("INSERT INTO ");
		out.Print(state.zDestTable);
		if (show_header) {
			out.Print("(");
			for (idx_t i = 0; i < col_names.size(); i++) {
				if (i > 0) {
					out.Print(",");
				}
				out.OutputQuotedIdentifier(col_names[i]);
			}
			out.Print(")");
		}
		for (idx_t i = 0; i < data.size(); i++) {
			out.Print(i > 0 ? "," : " VALUES(");
			if (is_null[i]) {
				out.Print("NULL");
			} else if (types[i].IsNumeric()) {
				out.Print(data[i]);
			} else if (state.ShellHasFlag(ShellFlags::SHFLG_Newlines)) {
				out.OutputQuotedString(data[i]);
			} else {
				out.OutputQuotedString(EscapeNewlines(data[i]));
			}
		}
		out.Print(");\n");
	}

	string EscapeNewlines(const string &str) {
		bool needs_quoting = false;
		bool needs_concat = false;
		for (auto c : str) {
			if (c == '\n' || c == '\r') {
				needs_quoting = true;
				needs_concat = true;
				break;
			}
			if (c == '\'') {
				needs_quoting = true;
			}
		}
		if (!needs_quoting) {
			return str;
		}
		string res;
		if (needs_concat) {
			res = "concat('";
		} else {
			res = "'";
		}
		for (auto c : str) {
			switch (c) {
			case '\n':
			case '\r':
				// newline - finish the current string literal and write the newline with a chr function
				res += "', chr(";
				if (c == '\n') {
					res += "10";
				} else {
					res += "13";
				}
				res += "), '";
				break;
			case '\'':
				// escape the quote
				res += "''";
				break;
			default:
				res += c;
				break;
			}
		}
		res += "'";
		if (needs_concat) {
			res += ")";
		}
		return res;
	}
};

class ModeSemiRenderer : public RowRenderer {
public:
	explicit ModeSemiRenderer(ShellState &state) : RowRenderer(state) {
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
		/* .schema and .fullschema output */
		out.Print(state.GetSchemaLine(row.data[0], "\n"));
	}
};

class ModePrettyRenderer : public RowRenderer {
public:
	explicit ModePrettyRenderer(ShellState &state) : RowRenderer(state) {
	}

	static bool IsSpace(char c) {
		return duckdb::StringUtil::CharacterIsSpace(c);
	}

	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override {
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
			out.Print(data[0]);
			out.Print(";\n");
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
						out.Print(state.GetSchemaLineN(z, j, "\n"));
						j = 0;
					}
				}
				z[j++] = c;
				if (nParen == 1 && cEnd == 0 && (c == '(' || c == '\n' || (c == ',' && !wsToEol(z + i + 1)))) {
					if (c == '\n')
						j--;
					out.Print(state.GetSchemaLineN(z, j, "\n  "));
					j = 0;
					nLine++;
					while (IsSpace(z[i + 1])) {
						i++;
					}
				}
			}
			z[j] = 0;
		}
		out.Print(state.GetSchemaLine(z, ";\n"));
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
	DuckBoxRenderer(PrintStream &out, ShellState &state, bool highlight)
	    : out(out), shell_highlight(state), output(PrintOutput::STDOUT), highlight(highlight) {
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
		if (highlight) {
			shell_highlight.PrintText(text, output, element_type);
		} else {
			out.Print(text);
		}
	}

private:
	PrintStream &out;
	ShellHighlight shell_highlight;
	PrintOutput output;
	bool highlight = true;
};

class ModeDuckBoxRenderer : public ShellRenderer {
public:
	explicit ModeDuckBoxRenderer(ShellState &state) : ShellRenderer(state) {
	}

	SuccessState RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) override;
	bool RequireMaterializedResult() const override {
		return true;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override {
		if (global_mode == PagerMode::PAGER_ON) {
			return true;
		}
		// in duckbox mode the output is automatically truncated to "max_rows"
		// if "max_rows" is smaller than pager_min_rows in this mode, we never show the pager
		if (state.max_rows < state.pager_min_rows && state.max_width == 0) {
			return false;
		}
		// FIXME: actually look at row count / render width?
		return true;
	}
};

SuccessState ModeDuckBoxRenderer::RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) {
	DuckBoxRenderer result_renderer(out, state, state.HighlightResults() && out.SupportsHighlight());
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
// Describe Renderer
//===--------------------------------------------------------------------===//
class ModeDescribeRenderer : public ShellRenderer {
public:
	explicit ModeDescribeRenderer(ShellState &state) : ShellRenderer(state) {
	}

	SuccessState RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) override;
	bool RequireMaterializedResult() const override {
		return true;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override {
		// pager gets handled separately (inside RenderTableMetadata)
		return false;
	}
};

SuccessState ModeDescribeRenderer::RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &res) {
	vector<ShellTableInfo> result;
	ShellTableInfo table;
	table.table_name = state.describe_table_name;
	for (auto &row : res.result) {
		ShellColumnInfo column;
		column.column_name = row.GetValue<string>(0);
		column.column_type = row.GetValue<string>(1);
		if (!row.IsNull(2)) {
			column.is_not_null = row.GetValue<string>(2) == "NO";
		}
		if (!row.IsNull(3)) {
			column.is_primary_key = row.GetValue<string>(3) == "PRI";
			column.is_unique = row.GetValue<string>(3) == "UNI";
		}
		if (!row.IsNull(4)) {
			column.default_value = row.GetValue<string>(4);
		}
		table.columns.push_back(std::move(column));
	}
	result.push_back(std::move(table));
	state.RenderTableMetadata(result);
	return SuccessState::SUCCESS;
}

//===--------------------------------------------------------------------===//
// Trash Renderer
//===--------------------------------------------------------------------===//
class ModeTrashRenderer : public ShellRenderer {
public:
	explicit ModeTrashRenderer(ShellState &state) : ShellRenderer(state) {
	}

	SuccessState RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) override {
		return SuccessState::SUCCESS;
	}
	bool RequireMaterializedResult() const override {
		return true;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override {
		// mode trash never uses the pager
		return false;
	}
};

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
	case RenderMode::DESCRIBE:
		return make_uniq<ModeDescribeRenderer>(*this);
	case RenderMode::TRASH:
		return make_uniq<ModeTrashRenderer>(*this);
	default:
		throw std::runtime_error("Unsupported mode for GetRenderer");
	}
}

} // namespace duckdb_shell
