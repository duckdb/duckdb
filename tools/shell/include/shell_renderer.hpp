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
struct RenderingResultIterator;

struct ResultMetadata {
	explicit ResultMetadata(duckdb::QueryResult &result);

	vector<string> column_names;
	vector<duckdb::LogicalType> types;
	vector<string> type_names;

	idx_t ColumnCount() const {
		return column_names.size();
	}
};

struct RowData {
	vector<string> data;
	vector<bool> is_null;
	idx_t row_index = 0;
};

struct RenderingQueryResult {
	RenderingQueryResult(duckdb::QueryResult &result, ShellRenderer &renderer)
	    : result(result), renderer(renderer), metadata(result), is_converted(false) {
	}

	duckdb::QueryResult &result;
	ShellRenderer &renderer;
	ResultMetadata metadata;
	vector<vector<string>> data;
	bool is_converted = false;

	idx_t ColumnCount() const {
		return metadata.ColumnCount();
	}

public:
	RenderingResultIterator begin(); // NOLINT: match stl API
	RenderingResultIterator end();   // NOLINT: match stl API
};

class ShellRenderer {
public:
	explicit ShellRenderer(ShellState &state);
	virtual ~ShellRenderer() = default;

	ShellState &state;
	bool show_header;
	string col_sep;
	string row_sep;

public:
	virtual SuccessState RenderQueryResult(ShellState &state, RenderingQueryResult &result);
	virtual void Analyze(RenderingQueryResult &result);
	virtual void RenderHeader(ResultMetadata &result);
	virtual void RenderRow(ResultMetadata &result, RowData &row);
	virtual void RenderFooter(ResultMetadata &result);
	virtual string NullValue();
	virtual bool RequireMaterializedResult() const = 0;
};

class ColumnRenderer : public ShellRenderer {
public:
	explicit ColumnRenderer(ShellState &state);

	void Analyze(RenderingQueryResult &result) override;
	virtual string ConvertValue(const char *value);
	void RenderRow(ResultMetadata &result, RowData &row) override;

	virtual const char *GetColumnSeparator() = 0;
	virtual const char *GetRowSeparator() = 0;
	virtual const char *GetRowStart() {
		return nullptr;
	}
	bool RequireMaterializedResult() const override {
		return true;
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
	virtual void RenderHeader(ResultMetadata &result) override;
	bool RequireMaterializedResult() const override {
		return false;
	}
};

} // namespace duckdb_shell
