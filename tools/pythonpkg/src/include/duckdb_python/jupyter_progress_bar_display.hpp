//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/jupyter_progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

class JupyterProgressBarDisplay : public ProgressBarDisplay {
public:
	JupyterProgressBarDisplay();
	virtual ~JupyterProgressBarDisplay() {
	}

	static unique_ptr<ProgressBarDisplay> Create();

public:
	void Update(double progress);
	void Finish();

private:
	void Initialize();

private:
	py::object progress_bar;
};

} // namespace duckdb
