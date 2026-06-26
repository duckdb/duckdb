#include "shell_highlight.hpp"
#include "shell_state.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/profiler_extension.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb_shell {

namespace {

//! Base sink that maps the tree renderer's typed segments onto the shell's highlight elements. Subclasses decide
//! whether the highlighted text is printed to the terminal or accumulated into a string.
class CLIHighlightRenderer : public duckdb::BaseResultRenderer {
public:
	void RenderLayout(const string &text) override {
		Emit(text, HighlightElementType::LAYOUT);
	}
	void RenderColumnName(const string &text) override {
		Emit(text, HighlightElementType::COLUMN_NAME);
	}
	void RenderType(const string &text) override {
		Emit(text, HighlightElementType::COLUMN_TYPE);
	}
	void RenderValue(const string &text, const duckdb::LogicalType &type) override {
		Emit(text, HighlightElementType::NUMERIC_VALUE);
	}
	void RenderNull(const string &text, const duckdb::LogicalType &type) override {
		Emit(text, HighlightElementType::NULL_VALUE);
	}
	void RenderFooter(const string &text) override {
		Emit(text, HighlightElementType::FOOTER);
	}

protected:
	virtual void Emit(const string &text, HighlightElementType type) = 0;
};

//! Prints the highlighted profiler tree directly to an output stream (used by PRAGMA enable_profiling).
class CLIResultRenderer : public CLIHighlightRenderer {
public:
	explicit CLIResultRenderer(PrintOutput output = PrintOutput::STDERR)
	    : highlight(ShellState::Get()), output(output) {
	}

protected:
	void Emit(const string &text, HighlightElementType type) override {
		auto &state = highlight.state;
		bool is_console = output == PrintOutput::STDOUT ? state.stdout_is_console : state.stderr_is_console;
		if (is_console) {
			// PrintText additionally honors the ".highlight" toggle
			highlight.PrintText(text, output, type);
		} else {
			// not a console - emit plain text so redirected output has no escape codes
			state.Print(output, text);
		}
	}

private:
	ShellHighlight highlight;
	PrintOutput output;
};

//! Accumulates the highlighted tree into a string (used for EXPLAIN, whose plan flows back as a result value).
class HighlightStringRenderer : public CLIHighlightRenderer {
public:
	const string &str() {
		return result;
	}

protected:
	void Emit(const string &text, HighlightElementType type) override {
		auto &element = ShellHighlight::GetHighlightElement(type);
		if (!ShellHighlight::IsEnabled() ||
		    (element.color == PrintColor::STANDARD && element.intensity == PrintIntensity::STANDARD)) {
			result += text;
			return;
		}
		result += ShellHighlight::TerminalCode(element.color, element.intensity);
		result += text;
		result += ShellHighlight::ResetTerminalCode();
	}

private:
	string result;
};

//! A text tree renderer whose print sink highlights the profiler output for the CLI (PRAGMA enable_profiling).
class CLITreeRenderer : public duckdb::TextTreeRenderer {
public:
	duckdb::unique_ptr<duckdb::BaseResultRenderer> GetPrintRenderer() override {
		return duckdb::make_uniq<CLIResultRenderer>(PrintOutput::STDERR);
	}
};

//! Renderer used for EXPLAIN / EXPLAIN ANALYZE in the CLI. The plan is rendered into a highlighted string that flows
//! back as the EXPLAIN result value and is printed by ModeExplainRenderer (which drops the section header).
class ShellExplainPrinter : public duckdb::TextTreeRenderer {
public:
	// plain EXPLAIN: the logical/physical operator tree
	void ToStreamInternal(duckdb::RenderTree &root, duckdb::BaseResultRenderer &ss) override {
		HighlightStringRenderer highlighted;
		duckdb::TextTreeRenderer::ToStreamInternal(root, highlighted);
		ss << highlighted.str();
	}

	// EXPLAIN ANALYZE: the framed query profiling tree
	void RenderProfiler(const duckdb::QueryProfiler &profiler, duckdb::BaseResultRenderer &ss) override {
		HighlightStringRenderer highlighted;
		profiler.RenderQueryTree(highlighted);
		ss << highlighted.str();
	}
};

} // namespace

void RegisterProfilerHighlighting(duckdb::DBConfig &config) {
	// PRAGMA enable_profiling output (printed directly to stderr)
	auto profiler_extension = duckdb::make_shared_ptr<duckdb::ProfilerExtension>();
	profiler_extension->create_renderer =
	    [](duckdb::ClientContext &context) -> duckdb::unique_ptr<duckdb::TreeRenderer> {
		auto renderer = duckdb::make_uniq<CLITreeRenderer>();
		renderer->Configure(duckdb::ClientConfig::GetConfig(context).profiling_renderer_settings);
		return std::move(renderer);
	};
	duckdb::ProfilerExtension::Register(config, "query_tree", std::move(profiler_extension));

	// EXPLAIN / EXPLAIN ANALYZE output (highlighted string, see ShellState::SetupPrettyExplain)
	auto explain_extension = duckdb::make_shared_ptr<duckdb::ProfilerExtension>();
	explain_extension->create_renderer =
	    [](duckdb::ClientContext &context) -> duckdb::unique_ptr<duckdb::TreeRenderer> {
		auto renderer = duckdb::make_uniq<ShellExplainPrinter>();
		renderer->Configure(duckdb::ClientConfig::GetConfig(context).profiling_renderer_settings);
		return std::move(renderer);
	};
	duckdb::ProfilerExtension::Register(config, "shell_explain_printer", std::move(explain_extension));
}

} // namespace duckdb_shell
