#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void ProgressBar::ProgressBarThread() {
#ifndef DUCKDB_NO_THREADS
	WaitFor(std::chrono::milliseconds(show_progress_after));
	while (!stop) {
		int new_percentage;
		supported = executor->GetPipelinesProgress(new_percentage);
		current_percentage = new_percentage;
		if (supported && current_percentage > -1 && ClientConfig::GetConfig(executor->context).print_progress_bar) {
			Printer::PrintProgress(current_percentage, PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
		WaitFor(std::chrono::milliseconds(time_update_bar));
	}
#endif
}

int ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
#ifndef DUCKDB_NO_THREADS
	Stop(false);
	stop = false;
	current_percentage = 0;
	progress_bar_thread = thread(&ProgressBar::ProgressBarThread, this);
#endif
}

ProgressBar::~ProgressBar() {
	Stop();
}

void ProgressBar::Stop(bool success) {
#ifndef DUCKDB_NO_THREADS
	if (progress_bar_thread.joinable()) {
		stop = true;
		c.notify_one();
		progress_bar_thread.join();
		if (success && supported && current_percentage > 0 &&
		    ClientConfig::GetConfig(executor->context).print_progress_bar) {
			Printer::FinishProgressBarPrint(PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
	}
#endif
}
} // namespace duckdb
