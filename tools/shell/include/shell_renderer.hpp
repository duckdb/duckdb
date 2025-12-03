//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"
#include "shell_highlight.hpp"

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
	    : result(result), renderer(renderer), metadata(result) {
	}

	duckdb::QueryResult &result;
	ShellRenderer &renderer;
	ResultMetadata metadata;
	vector<vector<string>> data;
	bool exhausted_result = false;

	idx_t ColumnCount() const {
		return metadata.ColumnCount();
	}
	bool TryConvertChunk(ShellRenderer &renderer);

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
	virtual string ConvertValue(const char *value);
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

class ShellLogStorage : public duckdb::LogStorage {
public:
	explicit ShellLogStorage(ShellState &state) : shell_highlight(state) {};

	~ShellLogStorage() override = default;

	const string GetStorageName() override {
		return "ShellLogStorage";
	}

protected:
	void WriteLogEntry(duckdb::timestamp_t timestamp, duckdb::LogLevel level, const string &log_type,
	                   const string &log_message, const duckdb::RegisteredLoggingContext &context) override;
	void WriteLogEntries(duckdb::DataChunk &chunk, const duckdb::RegisteredLoggingContext &context) override {};
	void FlushAll() override {};
	void Flush(duckdb::LoggingTargetTable table) override {};
	bool IsEnabled(duckdb::LoggingTargetTable table) override {
		return true;
	};

private:
	ShellHighlight shell_highlight;
};

} // namespace duckdb_shell
