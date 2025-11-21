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

struct ResultMetadata {
	explicit ResultMetadata(duckdb::QueryResult &result);

	vector<string> column_names;
	vector<duckdb::LogicalType> types;
	vector<string> type_names;

	idx_t ColumnCount() const {
		return column_names.size();
	}
};

struct ColumnarResult {
	explicit ColumnarResult(duckdb::QueryResult &result) : metadata(result) {}

	ResultMetadata metadata;
	vector<vector<string>> data;

	idx_t ColumnCount() const {
		return metadata.ColumnCount();
	}
};

struct RowData {
	vector<string> data;
	vector<bool> is_null;
	idx_t row_index = 0;
};


class ColumnRenderer : public ShellRenderer {
public:
	explicit ColumnRenderer(ShellState &state);

	void Analyze(ColumnarResult &result);
	virtual string ConvertValue(const char *value);
	virtual void RenderHeader(ResultMetadata &result) = 0;
	virtual void RenderRow(RowData &row);
	virtual void RenderFooter(ResultMetadata &result);

	virtual const char *GetColumnSeparator() = 0;
	virtual const char *GetRowSeparator() = 0;
	virtual const char *GetRowStart() {
		return nullptr;
	}

	void RenderAlignedValue(ResultMetadata &result, idx_t c);

protected:
	vector<idx_t> column_width;
	vector<bool> right_align;
};

class RowRenderer : public ShellRenderer {
public:
	explicit RowRenderer(ShellState &state);

public:
	virtual void RenderHeader(ResultMetadata &result);
	virtual void RenderRow(ResultMetadata &result, RowData &row) = 0;
	virtual void RenderFooter(ResultMetadata &result);
	virtual string NullValue();
};

} // namespace duckdb_shell
