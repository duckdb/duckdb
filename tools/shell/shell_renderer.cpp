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

ColumnRenderer::ColumnRenderer(ShellState &state) : state(state) {}

void ColumnRenderer::RenderFooter(ColumnarResult &result) {
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
			int w = result.column_width[i];
			int n = state.strlenChar(result.data[i]);
			state.Print(string((w - n) / 2, ' '));
			state.Print(result.data[i]);
			state.Print(string((w - n + 1) / 2, ' '));
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

unique_ptr<ColumnRenderer> ShellState::GetColumnRenderer() {
	switch(cMode) {
	case RenderMode::COLUMN:
		return unique_ptr<ColumnRenderer>(new ModeColumnRenderer(*this));
	case RenderMode::TABLE:
		return unique_ptr<ColumnRenderer>(new ModeTableRenderer(*this));
	case RenderMode::BOX:
	case RenderMode::MARKDOWN:
	case RenderMode::LATEX:
		return nullptr;
	default:
		throw std::runtime_error("Unsupported mode for GetColumnRenderer");
	}
}

}
