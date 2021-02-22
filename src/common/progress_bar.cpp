#include "duckdb/common/progress_bar.hpp"

namespace duckdb {
void ProgressBar::PrintProgress(int percentage) {
	int lpad = (int)(percentage / 100.0 * pbwidth);
	int rpad = pbwidth - lpad;
	printf("\r%3d%% [%.*s%*s]", percentage, lpad, pbstr.c_str(), rpad, "");
	fflush(stdout);
}

void ProgressBar::ProgressBarThread() {
	std::this_thread::sleep_for(std::chrono::milliseconds(show_progress_after));
	while (!StopRequested()) {
		PrintProgress(executor->GetPipelinesProgress());
		std::this_thread::sleep_for(std::chrono::milliseconds(time_update_bar));
	}
}

void ProgressBar::ProgressBarThreadTest() {
	std::this_thread::sleep_for(std::chrono::milliseconds(show_progress_after));
	while (!StopRequested()) {
		bool acceptable_percentage =
		    cur_percentage <= executor->GetPipelinesProgress() && executor->GetPipelinesProgress() <= 100;
		valid_percentage = valid_percentage && acceptable_percentage;
		cur_percentage = executor->GetPipelinesProgress();
		std::this_thread::sleep_for(std::chrono::milliseconds(time_update_bar));
	}
}

void ProgressBar::Start() {
	exit_signal = std::promise<void>();
	valid_percentage = true;
	cur_percentage = 0;
	if (running_test) {
		progress_bar_thread = std::thread(&ProgressBar::ProgressBarThreadTest, this);
	} else {
		progress_bar_thread = std::thread(&ProgressBar::ProgressBarThread, this);
	}
}

bool ProgressBar::StopRequested() {
	if (future_obj.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout) {
		return false;
	}
	return true;
}

void ProgressBar::Stop() {
	if (progress_bar_thread.joinable()) {
		exit_signal.set_value();
		progress_bar_thread.join();
		printf(" \n");
		fflush(stdout);
	}
}

bool ProgressBar::IsPercentageValid() {
	return valid_percentage;
}

} // namespace duckdb
