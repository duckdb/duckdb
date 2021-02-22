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
		int percentage = executor->GetPipelinesProgress(supported);
		if (supported && percentage > 0) {
			PrintProgress(percentage);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(time_update_bar));
	}
}

void ProgressBar::ProgressBarThreadTest() {
	std::this_thread::sleep_for(std::chrono::milliseconds(show_progress_after));
	while (!StopRequested()) {
		int percentage = executor->GetPipelinesProgress(supported);
		bool acceptable_percentage = cur_percentage <= percentage && percentage <= 100;
		valid_percentage = valid_percentage && acceptable_percentage;
		cur_percentage = percentage;
		std::this_thread::sleep_for(std::chrono::milliseconds(time_update_bar));
	}
}

void ProgressBar::Start() {
	exit_signal = std::promise<void>();
	future_obj = exit_signal.get_future();
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
		if (!running_test || !supported) {
			printf(" \n");
			fflush(stdout);
		}
	}
}

bool ProgressBar::IsPercentageValid() {
	return valid_percentage;
}

} // namespace duckdb
