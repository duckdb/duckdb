#include "duckdb/common/progress_bar.hpp"

namespace duckdb {

void ProgressBar::ProgressBarThread() {
	WaitFor(std::chrono::milliseconds(show_progress_after));
	while (!stop) {
		auto new_percentage = executor->GetPipelinesProgress(supported);
		if (new_percentage > 100 || new_percentage < cur_percentage) {
			valid_percentage = false;
		}
		cur_percentage = new_percentage;
		if (supported && cur_percentage > -1) {
			PrintProgress(cur_percentage);
		}
		WaitFor(std::chrono::milliseconds(100));
	}
}

int ProgressBar::GetCurPercentage() {
	return cur_percentage;
}

bool ProgressBar::IsPercentageValid() {
	return valid_percentage;
}

void ProgressBar::Start() {
#ifndef DUCKDB_NO_THREADS
	valid_percentage = true;
	cur_percentage = 0;
	progress_bar_thread = std::thread(&ProgressBar::ProgressBarThread, this);
#endif
}

void ProgressBar::Stop() {
#ifndef DUCKDB_NO_THREADS
	if (progress_bar_thread.joinable()) {
		{
			std::lock_guard<std::mutex> l(m);
			stop = true;
		}
		c.notify_one();
		progress_bar_thread.join();
		if (supported) {
			FinishPrint();
		}
	}
#endif
}
} // namespace duckdb
