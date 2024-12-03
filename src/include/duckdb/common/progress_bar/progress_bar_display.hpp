//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
	virtual void AddInfo(const char *name, double value) {
	}
};

} // namespace duckdb
