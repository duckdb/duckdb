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
	virtual void AddTextualInfo(const string &tag, const string &info) {
	}
	virtual void AddNumericInfo(const string &tag, double info) {
	}
};

} // namespace duckdb
