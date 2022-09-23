#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after, bool print_progress)
    : executor(executor), show_progress_after(show_progress_after), current_percentage(-1),
      print_progress(print_progress) {
}

double ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
	profiler.Start();
	current_percentage = 0;
	supported = true;
}

void ProgressBar::Update(bool final) {
	if (!supported) {
		return;
	}
	double new_percentage;
	supported = executor.GetPipelinesProgress(new_percentage);
	if (!supported) {
		return;
	}
	auto sufficient_time_elapsed = profiler.Elapsed() > show_progress_after / 1000.0;
	if (new_percentage > current_percentage) {
		current_percentage = new_percentage;
	}
	if (supported && print_progress && sufficient_time_elapsed && current_percentage > -1 && print_progress) {
		if (final) {
			Printer::FinishProgressBarPrint(PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		} else {
			Printer::PrintProgress(current_percentage, PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
	}
}

} // namespace duckdb
