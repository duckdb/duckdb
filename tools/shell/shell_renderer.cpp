#include "shell_renderer.hpp"

#include "shell_state.hpp"

namespace duckdb_shell {

bool ShellRenderer::IsColumnar(RenderMode mode) {
	switch(mode) {
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

//===--------------------------------------------------------------------===//
// Column Renderers
//===--------------------------------------------------------------------===//
ColumnRenderer::ColumnRenderer(ShellState &state) : state(state) {}

void ColumnRenderer::RenderFooter(ColumnarResult &result) {
}

void ColumnRenderer::RenderAlignedValue(ColumnarResult &result, idx_t i) {
	int w = result.column_width[i];
	int n = state.strlenChar(result.data[i]);
	state.PrintPadded("", (w - n) / 2);
	state.Print(result.data[i]);
	state.PrintPadded("", (w - n + 1) / 2);
}

class ModeColumnRenderer : public ColumnRenderer {
public:
	explicit ModeColumnRenderer(ShellState &state) : ColumnRenderer(state) {}

	void RenderHeader(ColumnarResult &result) override {
		if( !state.showHeader ) {
			return;
		}
		for(idx_t i=0; i<result.column_count; i++){
			state.utf8_width_print(state.out, result.column_width[i], result.data[i]);
			state.Print(i==result.column_count-1?"\n":"  ");
		}
		for(idx_t i=0; i<result.column_count; i++){
			state.print_dashes(result.column_width[i]);
			state.Print(i==result.column_count-1?"\n":"  ");
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
	explicit ModeTableRenderer(ShellState &state) : ColumnRenderer(state) {}

	void RenderHeader(ColumnarResult &result) override {
		state.print_row_separator(result.column_count, "+", result.column_width);
		state.Print("| ");
		for(idx_t i=0; i<result.column_count; i++){
			RenderAlignedValue(result, i);
			state.Print(i==result.column_count-1?" |\n":" | ");
		}
		state.print_row_separator(result.column_count, "+", result.column_width);
	}

	void RenderFooter(ColumnarResult &result) override {
		state.print_row_separator(result.column_count, "+", result.column_width);
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
	explicit ModeMarkdownRenderer(ShellState &state) : ColumnRenderer(state) {}

	void RenderHeader(ColumnarResult &result) override {
		for(idx_t i=0; i<result.column_count; i++){
			RenderAlignedValue(result, i);
		}
		state.print_markdown_separator(result.column_count, "|", result.types, result.column_width);
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
#define BOX_24   "\342\224\200"  /* U+2500 --- */
#define BOX_13   "\342\224\202"  /* U+2502  |  */
#define BOX_23   "\342\224\214"  /* U+250c  ,- */
#define BOX_34   "\342\224\220"  /* U+2510 -,  */
#define BOX_12   "\342\224\224"  /* U+2514  '- */
#define BOX_14   "\342\224\230"  /* U+2518 -'  */
#define BOX_123  "\342\224\234"  /* U+251c  |- */
#define BOX_134  "\342\224\244"  /* U+2524 -|  */
#define BOX_234  "\342\224\254"  /* U+252c -,- */
#define BOX_124  "\342\224\264"  /* U+2534 -'- */
#define BOX_1234 "\342\224\274"  /* U+253c -|- */

class ModeBoxRenderer : public ColumnRenderer {
public:
	explicit ModeBoxRenderer(ShellState &state) : ColumnRenderer(state) {}

	void RenderHeader(ColumnarResult &result) override {
		print_box_row_separator(result.column_count, BOX_23, BOX_234, BOX_34, result.column_width);
		state.Print(BOX_13 " ");
		for(idx_t i=0; i<result.column_count; i++){
			RenderAlignedValue(result, i);
			state.Print(i==result.column_count-1?" " BOX_13 "\n":" " BOX_13 " ");
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
		return BOX_13" ";
	}

private:
	/* Draw horizontal line N characters long using unicode box
	** characters
	*/
	void print_box_line(int N){
		string box_line;
		for(idx_t i = 0; i < N; i++) {
			box_line += BOX_24;
		}
		state.Print(box_line);
	}

	/*
	** Draw a horizontal separator for a RenderMode::Box table.
	*/
	void print_box_row_separator(
	  int nArg,
	  const char *zSep1,
	  const char *zSep2,
	  const char *zSep3,
	  const vector<int> &actualWidth
	){
		int i;
		if( nArg>0 ){
			state.Print(zSep1);
			print_box_line(actualWidth[0]+2);
			for(i=1; i<nArg; i++){
				state.Print(zSep2);
				print_box_line(actualWidth[i]+2);
			}
			state.Print(zSep3);
		}
		state.Print("\n");
	}
};

class ModeLatexRenderer : public ColumnRenderer {
public:
	explicit ModeLatexRenderer(ShellState &state) : ColumnRenderer(state) {}

	void RenderHeader(ColumnarResult &result) override {
		state.Print("\\begin{tabular}{|");
		for(idx_t i=0; i<result.column_count; i++){
			if (state.column_type_is_integer(result.type_names[i])) {
				state.Print("r");
			} else {
				state.Print("l");
			}
		}
		state.Print("|}\n");
		state.Print("\\hline\n");
		for(idx_t i=0; i<result.column_count; i++){
			RenderAlignedValue(result, i);
			state.Print(i==result.column_count-1? GetRowSeparator():GetColumnSeparator());
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
	switch(cMode) {
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
RowRenderer::RowRenderer(ShellState &state) : state(state) {}

void RowRenderer::Render(RowResult &result) {
	if (first_row) {
		RenderHeader(result);
		first_row = false;
	}
	RenderRow(result);
}

void RowRenderer::RenderHeader(RowResult &result) {}

void RowRenderer::RenderFooter(RowResult &result) {}

class ModeLineRenderer : public RowRenderer {
public:
	explicit ModeLineRenderer(ShellState &state) : RowRenderer(state) {}

	void Render(RowResult &result) override {
		if (first_row) {
			auto &col_names = result.column_names;
			// determine the render width by going over the column names
			w = 5;
			for(idx_t i=0; i<col_names.size(); i++){
				int len = ShellState::StringLength(col_names[i] ? col_names[i] : "");
				if( len>w ) w = len;
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
		for(idx_t i=0; i<data.size(); i++){
			state.PrintPadded(col_names[i], w);
			state.Print(" = ");
			state.Print(data[i] ? data[i] : state.nullValue);
			state.Print(state.rowSeparator);
		}
	}

	int w = 0;
};


unique_ptr<RowRenderer> ShellState::GetRowRenderer() {
	switch(cMode) {
	case RenderMode::LINE:
		return unique_ptr<RowRenderer>(new ModeLineRenderer(*this));
	case RenderMode::TRASH:
		// no renderer
		return nullptr;
	default:
		throw std::runtime_error("Unsupported mode for GetRowRenderer");
	}
}

}
