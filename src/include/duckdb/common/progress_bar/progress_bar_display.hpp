//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

class ProgressBarDisplay {
public:
	ProgressBarDisplay() {
	}
	virtual ~ProgressBarDisplay() {
	}

public:
	virtual void Update(double percentage) = 0;
	virtual void Finish() = 0;
	//! Shows a message on what the statement is doing (e.g. "Installing extension httpfs", see ClientStatus): next to
	//! the progress, or on its own while there is no progress (yet). An empty message removes it. May be called from
	//! any thread, but never concurrently with the other calls. Displays that do not show messages ignore it
	virtual void UpdateStatus(const string &message) {
	}
};

} // namespace duckdb
