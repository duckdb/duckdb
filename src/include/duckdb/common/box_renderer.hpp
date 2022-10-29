//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/box_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {
class ColumnDataCollection;

enum class ValueRenderAlignment {
	LEFT,
	MIDDLE,
	RIGHT
};

struct BoxRendererConfig {
	// a max_width of 0 means we default to the terminal width
	idx_t max_width = 0;
	idx_t max_rows = 20;
	// the max col width determines the maximum size of a single column
	// note that the max col width is only used if the result does not fit on the screen
	idx_t max_col_width = 20;

#ifndef DUCKDB_ASCII_TREE_RENDERER
//	const char *LTCORNER = "\342\224\214"; // "┌";
//	const char *RTCORNER = "\342\224\220"; // "┐";
//	const char *LDCORNER = "\342\224\224"; // "└";
//	const char *RDCORNER = "\342\224\230"; // "┘";
	const char *LTCORNER = "╭"; // "┌";
	const char *RTCORNER = "╮"; // "┐";
	const char *LDCORNER = "╰"; // "└";
	const char *RDCORNER = "╯"; // "┘";

	const char *MIDDLE = "\342\224\274";  // "┼";
	const char *TMIDDLE = "\342\224\254"; // "┬";
	const char *LMIDDLE = "\342\224\234"; // "├";
	const char *RMIDDLE = "\342\224\244"; // "┤";
	const char *DMIDDLE = "\342\224\264"; // "┴";

	const char *VERTICAL = "\342\224\202";   // "│";
	const char *HORIZONTAL = "\342\224\200"; // "─";

#else
	// ASCII version
	const char *LTCORNER = "<";
	const char *RTCORNER = ">";
	const char *LDCORNER = "<";
	const char *RDCORNER = ">";

	const char *MIDDLE = "+";
	const char *TMIDDLE = "+";
	const char *LMIDDLE = "+";
	const char *RMIDDLE = "+";
	const char *DMIDDLE = "+";

	const char *VERTICAL = "|";
	const char *HORIZONTAL = "-";
#endif
};

class BoxRenderer {
public:
	explicit BoxRenderer(BoxRendererConfig config_p = BoxRendererConfig());

	string ToString(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op);

	void Render(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op, std::ostream &ss);
	void Print(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op);

private:
	//! The configuration used for rendering
	BoxRendererConfig config;

private:
	void RenderValue(std::ostream &ss, const string &value, idx_t column_width, ValueRenderAlignment alignment = ValueRenderAlignment::MIDDLE);
	string RenderType(const LogicalType &type);
	ValueRenderAlignment TypeAlignment(const LogicalType &type);
};

} // namespace duckdb
