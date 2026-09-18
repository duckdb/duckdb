//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/tree_renderer/base_tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

//! Semantic category of a styled segment emitted while rendering a plan/profiler tree. A BaseTreeRenderer decides
//! how each category is realized: plain text, direct terminal output, or syntax highlighting (in the CLI).
enum class TreeRenderType {
	//! Box-drawing characters, padding and connector lines
	LAYOUT,
	//! Operator / node name (generic, when the category is unknown)
	NODE_NAME,
	//! Table / table-function scan operator name
	NODE_NAME_SCAN,
	//! Join / cross-product operator name
	NODE_NAME_JOIN,
	//! Aggregate / group-by / distinct / window operator name
	NODE_NAME_AGGREGATE,
	//! Order-by / top-n operator name
	NODE_NAME_ORDER,
	//! A detail key / metric label (e.g. "Condition", an estimated row count)
	KEY,
	//! A detail value / generic content
	VALUE,
	//! An actual (measured) row count
	ROWS,
	//! Operator timing taking a very large / large / moderate / small share of the total query time
	TIMING_CRITICAL,
	TIMING_HIGH,
	TIMING_MODERATE,
	TIMING_LOW,
	//! A section / plan header
	HEADER
};

//! Sink that a TreeRenderer renders into. Each segment carries a TreeRenderType describing what it represents, so
//! the sink can render it as plain text, print it directly, or apply syntax highlighting.
class BaseTreeRenderer {
public:
	virtual ~BaseTreeRenderer() {
	}

	virtual void Render(const string &text, TreeRenderType type) = 0;

	//! Set by the renderer when it omitted content that a full (unfolded) render would include - i.e. it condensed or
	//! merged low-impact operators. Lets a caller offer an "expand" affordance only when there is more to show.
	bool hidden_content = false;
	//! Set by the renderer to the widest rendered line (in display columns). Lets a caller decide whether the output
	//! fits on screen horizontally.
	idx_t max_render_width = 0;

	//! Convenience for emitting layout (box-drawing / padding) text
	BaseTreeRenderer &operator<<(const string &text) {
		Render(text, TreeRenderType::LAYOUT);
		return *this;
	}
	BaseTreeRenderer &operator<<(const char *text) {
		Render(string(text), TreeRenderType::LAYOUT);
		return *this;
	}
	BaseTreeRenderer &operator<<(char c) {
		Render(string(1, c), TreeRenderType::LAYOUT);
		return *this;
	}
};

//! Accumulates the rendered tree into a plain string, ignoring the segment types.
class StringTreeRenderer : public BaseTreeRenderer {
public:
	void Render(const string &text, TreeRenderType type) override {
		result += text;
	}
	const string &str() { // NOLINT: mimic string stream
		return result;
	}

private:
	string result;
};

//! Prints the rendered tree directly to an output stream, ignoring the segment types.
class PrinterTreeRenderer : public BaseTreeRenderer {
public:
	explicit PrinterTreeRenderer(OutputStream stream = OutputStream::STREAM_STDERR) : stream(stream) {
	}
	void Render(const string &text, TreeRenderType type) override {
		Printer::RawPrint(stream, text);
	}

private:
	OutputStream stream;
};

} // namespace duckdb
