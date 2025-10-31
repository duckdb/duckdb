//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"

namespace duckdb_shell {
struct ShellState;

class ShellRenderer {
public:
	explicit ShellRenderer(ShellState &state);
	virtual ~ShellRenderer() = default;

	ShellState &state;
	bool show_header;
	string col_sep;
	string row_sep;

public:
	static bool IsColumnar(RenderMode mode);
};

struct ColumnarResult {
	idx_t column_count = 0;
	vector<string> data;
	vector<duckdb::LogicalType> types;
	vector<idx_t> column_width;
	vector<bool> right_align;
	vector<string> type_names;
};

struct RowResult {
	vector<string> column_names;
	vector<string> data;
	vector<duckdb::LogicalType> types;
	vector<bool> is_null;
};

class ColumnRenderer : public ShellRenderer {
public:
	explicit ColumnRenderer(ShellState &state);

	virtual string ConvertValue(const char *value);
	virtual void RenderHeader(ColumnarResult &result) = 0;
	virtual void RenderFooter(ColumnarResult &result);

	virtual const char *GetColumnSeparator() = 0;
	virtual const char *GetRowSeparator() = 0;
	virtual const char *GetRowStart() {
		return nullptr;
	}

	void RenderAlignedValue(ColumnarResult &result, idx_t i);
};

class RowRenderer : public ShellRenderer {
public:
	explicit RowRenderer(ShellState &state);

	bool first_row = true;

public:
	virtual void Render(RowResult &result);

	virtual void RenderHeader(RowResult &result);
	virtual void RenderRow(RowResult &result) = 0;
	virtual void RenderFooter(RowResult &result);

	virtual string NullValue();
};

} // namespace duckdb_shell
