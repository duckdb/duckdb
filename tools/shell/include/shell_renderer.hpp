//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <shell_highlight.hpp>

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
	static bool IsColumnar(RenderMode mode);
	virtual string NullValue();
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
};

class ModeDuckBoxRenderer : public ShellRenderer {
public:
	explicit ModeDuckBoxRenderer(ShellState &state);

	SuccessState RenderQueryResult(ShellState &state, RenderingQueryResult &result) override;
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
