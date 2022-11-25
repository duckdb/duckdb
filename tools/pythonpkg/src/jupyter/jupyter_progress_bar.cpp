#include "duckdb_python/jupyter_progress_bar.hpp"
#include "duckdb_python/pyconnection.hpp"

namespace duckdb {

JupyterProgressBar::JupyterProgressBar() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto float_progress_attr = import_cache.ipywidgets.FloatProgress();
	D_ASSERT(float_progress_attr.ptr() != nullptr);
	// Initialize the progress bar
	py::dict style;
	style["bar_color"] = "black";
	progress_bar = float_progress_attr((py::arg("min") = 0, py::arg("max") = 100, py::arg("style") = style));

	progress_bar.attr("layout").attr("width") = "100%";

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
