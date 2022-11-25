#include "duckdb_python/jupyter_progress_bar.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {

// FIXME: Maybe this should be a ProgressBarDisplay, pluggable into ProgressBar
JupyterProgressBar::JupyterProgressBar() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto float_progress_attr = import_cache.ipywidgets.FloatProgress();
	D_ASSERT(float_progress_attr.ptr() != nullptr);
	// Initialize the progress bar
	progress_bar = float_progress_attr((py::arg("min") = py::cast(0), py::arg("max") = py::cast(100)));

	// Display the progress bar
	auto display_attr = import_cache.IPython.display.display();
	D_ASSERT(display_attr.ptr() != nullptr);
	display_attr(progress_bar);
}

void JupyterProgressBar::Update(double progress) {
	progress_bar.attr("value") = py::cast(progress);
}

void JupyterProgressBar::Finish() {
	Update(100);
}

} // namespace duckdb
