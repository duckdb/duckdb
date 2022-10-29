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
#include "duckdb/common/list.hpp"

namespace duckdb {
class ColumnDataCollection;
class ColumnDataRowCollection;

enum class ValueRenderAlignment { LEFT, MIDDLE, RIGHT };

struct BoxRendererConfig {
	// a max_width of 0 means we default to the terminal width
	idx_t max_width = 0;
	idx_t max_rows = 20;
	// the max col width determines the maximum size of a single column
	// note that the max col width is only used if the result does not fit on the screen
	idx_t max_col_width = 20;
	string null_value = "NULL";

#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *LTCORNER = "\xE2\x95\xAD"; // "╭";
	const char *RTCORNER = "\xE2\x95\xAE"; // "╮";
	const char *LDCORNER = "\xE2\x95\xB0"; // "╰";
	const char *RDCORNER = "\xE2\x95\xAF"; // "╯";

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
	static const idx_t SPLIT_COLUMN;

public:
	explicit BoxRenderer(BoxRendererConfig config_p = BoxRendererConfig());

	string ToString(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op);

	void Render(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op, std::ostream &ss);
	void Print(ClientContext &context, const vector<string> &names, const ColumnDataCollection &op);

private:
	//! The configuration used for rendering
	BoxRendererConfig config;

private:
	void RenderValue(std::ostream &ss, const string &value, idx_t column_width,
	                 ValueRenderAlignment alignment = ValueRenderAlignment::MIDDLE);
	string RenderType(const LogicalType &type);
	ValueRenderAlignment TypeAlignment(const LogicalType &type);
	string GetRenderValue(ColumnDataRowCollection &rows, idx_t c, idx_t r);

	list<ColumnDataCollection> FetchRenderCollections(ClientContext &context, const ColumnDataCollection &result,
	                                                  idx_t top_rows, idx_t bottom_rows);
	vector<idx_t> ComputeRenderWidths(const vector<string> &names, const ColumnDataCollection &result,
	                                  list<ColumnDataCollection> &collections, idx_t min_width, idx_t max_width,
	                                  vector<idx_t> &column_map, idx_t &total_length);
	void RenderHeader(const vector<string> &names, const vector<LogicalType> &result_types,
	                  const vector<idx_t> &column_map, const vector<idx_t> &widths, const vector<idx_t> &boundaries,
	                  idx_t total_length, bool has_results, std::ostream &ss);
	void RenderValues(const list<ColumnDataCollection> &collections, const vector<idx_t> &column_map,
	                  const vector<idx_t> &widths, const vector<LogicalType> &result_types, std::ostream &ss);
	void RenderRowCount(string row_count_str, string shown_str, const string &column_count_str,
	                    const vector<idx_t> &boundaries, bool has_hidden_rows, idx_t total_length, idx_t row_count,
	                    idx_t column_count, idx_t minimum_row_length, std::ostream &ss);
};

} // namespace duckdb
