#pragma once

#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

class JupyterProgressBar {
public:
	JupyterProgressBar();
	virtual ~JupyterProgressBar() {
	}

public:
	void Update(double progress);
	void Finish();

private:
	py::object progress_bar;
};

} // namespace duckdb
