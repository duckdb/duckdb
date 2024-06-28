#include "duckdb_python/jupyter_progress_bar_display.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

unique_ptr<ProgressBarDisplay> JupyterProgressBarDisplay::Create() {
	return make_uniq<JupyterProgressBarDisplay>();
}

void JupyterProgressBarDisplay::Initialize() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto float_progress_attr = import_cache.ipywidgets.FloatProgress();
	D_ASSERT(float_progress_attr.ptr() != nullptr);
	// Initialize the progress bar
	py::dict style;
	style["bar_color"] = "black";
	progress_bar = float_progress_attr((py::arg("min") = 0, py::arg("max") = 100, py::arg("style") = style));

	progress_bar.attr("layout").attr("width") = "auto";

	// Display the progress bar
	auto display_attr = import_cache.IPython.display.display();
	D_ASSERT(display_attr.ptr() != nullptr);
	display_attr(progress_bar);
}

JupyterProgressBarDisplay::JupyterProgressBarDisplay() : ProgressBarDisplay() {
	// Empty, we need the GIL to initialize, which we don't have here
}

void JupyterProgressBarDisplay::Update(double progress) {
	py::gil_scoped_acquire gil;
	if (progress_bar.ptr() == nullptr) {
		// First print, we first need to initialize the display
		Initialize();
	}
	progress_bar.attr("value") = py::cast(progress);
}

void JupyterProgressBarDisplay::Finish() {
	Update(100);
}

} // namespace duckdb
