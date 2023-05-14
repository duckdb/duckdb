#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"

namespace duckdb {

void ProgressBar::SystemOverrideCheck(ClientConfig &config) {
	if (config.system_progress_bar_disable_reason != nullptr) {
		throw InvalidInputException("Could not change the progress bar setting because: '%s'",
		                            config.system_progress_bar_disable_reason);
	}
}

unique_ptr<ProgressBarDisplay> ProgressBar::DefaultProgressBarDisplay() {
	return make_uniq<TerminalProgressBarDisplay>();
}

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after,
                         progress_bar_display_create_func_t create_display_func)
    : executor(executor), show_progress_after(show_progress_after), current_percentage(-1) {
	if (create_display_func) {
		display = create_display_func();
	}
}

double ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
	profiler.Start();
	current_percentage = 0;
	supported = true;
}

bool ProgressBar::PrintEnabled() const {
	return display != nullptr;
}

bool ProgressBar::ShouldPrint(bool final) const {
	if (!PrintEnabled()) {
		// Don't print progress at all
		return false;
	}
	// FIXME - do we need to check supported before running `profiler.Elapsed()` ?
	auto sufficient_time_elapsed = profiler.Elapsed() > show_progress_after / 1000.0;
	if (!sufficient_time_elapsed) {
		// Don't print yet
		return false;
	}
	if (final) {
		// Print the last completed bar
		return true;
	}
	if (!supported) {
		return false;
	}
	return current_percentage > -1;
}

void ProgressBar::Update(bool final) {
	if (!final && !supported) {
		return;
	}
	double new_percentage;
	supported = executor.GetPipelinesProgress(new_percentage);
	if (!final && !supported) {
		return;
	}
	if (new_percentage > current_percentage) {
		current_percentage = new_percentage;
	}
	if (ShouldPrint(final)) {
#ifndef DUCKDB_DISABLE_PRINT
		if (final) {
			FinishProgressBarPrint();
		} else {
			PrintProgress(current_percentage);
		}
#endif
	}
}

void ProgressBar::PrintProgress(int current_percentage) {
	D_ASSERT(display);
	display->Update(current_percentage);
}

void ProgressBar::FinishProgressBarPrint() {
	if (finished) {
		return;
	}
	D_ASSERT(display);
	display->Finish();
	finished = true;
}

} // namespace duckdb
