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
	static bool IsColumnar(RenderMode mode);
};

struct ColumnarResult {
	idx_t column_count = 0;
	vector<string> data;
	vector<int> types;
	vector<int> column_width;
};

class ColumnRenderer {
public:
	explicit ColumnRenderer(ShellState &state);
	virtual ~ColumnRenderer() = default;

	virtual void RenderHeader(ColumnarResult &result) = 0;
	virtual void RenderFooter(ColumnarResult &result);

	virtual const char *GetColumnSeparator() = 0;
	virtual const char *GetRowSeparator() = 0;
	virtual const char *GetRowStart() {
		return nullptr;
	}
protected:
	ShellState &state;
};


}
