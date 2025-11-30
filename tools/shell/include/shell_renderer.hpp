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
	vector<duckdb::string_t> data;
	vector<bool> is_null;
	idx_t row_index = 0;
};

struct RenderingQueryResult {
	RenderingQueryResult(duckdb::QueryResult &result, ShellRenderer &renderer);

	duckdb::QueryResult &result;
	ShellRenderer &renderer;
	ResultMetadata metadata;
	vector<unique_ptr<duckdb::DataChunk>> chunks;
	bool exhausted_result = false;
	idx_t loaded_row_count = 0;

	idx_t ColumnCount() const {
		return metadata.ColumnCount();
	}
	bool TryConvertChunk();

public:
	RenderingResultIterator begin(); // NOLINT: match stl API
	RenderingResultIterator end();   // NOLINT: match stl API
};

enum class TextAlignment { CENTER, LEFT, RIGHT };

struct PrintStream {
public:
	explicit PrintStream(ShellState &state);
	virtual ~PrintStream() = default;

	virtual void Print(const string &str) {
		state.Print(str);
	}
	virtual void Print(duckdb::string_t str) {
		state.Print(str);
	}
	virtual void Print(const char *str) {
		state.Print(str);
	}
	virtual void SetBinaryMode() {
		state.SetBinaryMode();
	}
	virtual void SetTextMode() {
		state.SetTextMode();
	}
	virtual bool SupportsHighlight() {
		return true;
	}

	void RenderAlignedValue(const string &str, idx_t width, TextAlignment alignment = TextAlignment::CENTER);
	void RenderAlignedValue(const char *str, idx_t str_len, idx_t width,
	                        TextAlignment alignment = TextAlignment::CENTER);
	void RenderAlignedValue(duckdb::string_t str, idx_t width, TextAlignment alignment = TextAlignment::CENTER);
	void PrintDashes(idx_t N);
	void OutputQuotedIdentifier(const string &str);
	void OutputQuotedString(const string &str);

public:
	ShellState &state;
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
	virtual SuccessState RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result);
	virtual void Analyze(RenderingQueryResult &result);
	virtual void RenderHeader(PrintStream &out, ResultMetadata &result);
	virtual void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row);
	virtual void RenderFooter(PrintStream &out, ResultMetadata &result);
	virtual string NullValue();
	virtual bool RequireMaterializedResult() const = 0;
	virtual bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) = 0;
};

class ColumnRenderer : public ShellRenderer {
public:
	explicit ColumnRenderer(ShellState &state);

	void Analyze(RenderingQueryResult &result) override;
	void RenderRow(PrintStream &out, ResultMetadata &result, RowData &row) override;

	virtual const char *GetColumnSeparator() = 0;
	virtual const char *GetRowSeparator() = 0;
	virtual const char *GetRowStart() {
		return nullptr;
	}
	bool RequireMaterializedResult() const override {
		return true;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override;
	virtual string ConvertValue(const char *value, idx_t str_len);
	virtual bool HasConvertValue() {
		return false;
	}

protected:
	vector<idx_t> column_width;
	vector<bool> right_align;
};

class RowRenderer : public ShellRenderer {
public:
	explicit RowRenderer(ShellState &state);

public:
	void RenderHeader(PrintStream &out, ResultMetadata &result) override;
	bool RequireMaterializedResult() const override {
		return false;
	}
	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override;
};

} // namespace duckdb_shell
