#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"
#include <thread>

namespace duckdb {

#define UPDATE_INTERVAL_MS 100

int32_t TerminalProgressBarDisplay::NormalizePercentage(double percentage) {
	if (percentage > 100) {
		return 100;
	}
	if (percentage < 0) {
		return 0;
	}
	return int32_t(percentage);
}

static std::string format_eta(double seconds, bool elapsed = false) {
	// Desired formats:
	//   00:00:00.00 remaining
	//   unknown     remaining
	//   00:00:00.00 elapsed
	const char *suffix = elapsed ? " elapsed)  " : " remaining)";

	if (seconds < 0 || seconds > 3600 * 99) {
		// Invalid or unknown ETA, or if its longer than 99 hours.
		return StringUtil::Format("(%11s%s", "unknown", suffix);
	}

	// Round to nearest centisecond as integer
	auto total_centiseconds = static_cast<uint64_t>(std::llround(seconds * 100.0));

	// Split into seconds and centiseconds
	uint64_t total_seconds = total_centiseconds / 100;
	uint32_t centiseconds = static_cast<uint32_t>(total_centiseconds % 100);

	// Break down total_seconds
	uint64_t hours = total_seconds / 3600;
	uint32_t minutes = static_cast<uint32_t>((total_seconds % 3600) / 60);
	uint32_t secs = static_cast<uint32_t>(total_seconds % 60);

	return StringUtil::Format("(%02llu:%02u:%02u.%02llu%s", hours, minutes, secs, centiseconds, suffix);
}

void TerminalProgressBarDisplay::PrintProgressInternal(int32_t percentage, double seconds, bool finished) {
	if (!run_periodic_updates && !finished) {
		run_periodic_updates = true;

		// Since the progress reports may not come at a regular interval,
		// we start a thread that will periodically update the display,
		// with the new estimated completion time, but we're doing this
		// here since we only want periodic updates if the progress bar is
		// being displayed.
		periodic_update_thread = std::thread([this]() { PeriodicUpdate(); });
	}
	string result;
	// we divide the number of blocks by the percentage
	// 0%   = 0
	// 100% = PROGRESS_BAR_WIDTH
	// the percentage determines how many blocks we need to draw
	double blocks_to_draw = PROGRESS_BAR_WIDTH * (percentage / 100.0);
	// because of the power of unicode, we can also draw partial blocks

	// render the percentage with some padding to ensure everything stays nicely aligned
	result = "\r";
	if (percentage < 100) {
		result += " ";
	}
	if (percentage < 10) {
		result += " ";
	}
	result += to_string(percentage) + "%";
	result += " ";
	result += format_eta(seconds, finished);
	result += " ";
	result += PROGRESS_START;
	idx_t i;
	for (i = 0; i < idx_t(blocks_to_draw); i++) {
		result += PROGRESS_BLOCK;
	}
	if (i < PROGRESS_BAR_WIDTH) {
		// print a partial block based on the percentage of the progress bar remaining
		idx_t index = idx_t((blocks_to_draw - static_cast<double>(idx_t(blocks_to_draw))) * PARTIAL_BLOCK_COUNT);
		if (index >= PARTIAL_BLOCK_COUNT) {
			index = PARTIAL_BLOCK_COUNT - 1;
		}
		result += PROGRESS_PARTIAL[index];
		i++;
	}
	for (; i < PROGRESS_BAR_WIDTH; i++) {
		result += PROGRESS_EMPTY;
	}
	result += PROGRESS_END;
	result += " ";

	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

void TerminalProgressBarDisplay::Update(double percentage) {
	std::lock_guard<std::mutex> lock(mtx);
	const double current_time = GetElapsedDuration();

	if (current_time - last_update_time >= UPDATE_INTERVAL_MS / 1000.0) {
		// Filters go from 0 to 1, percentage is from 0-100
		const double filter_percentage = percentage / 100.0;
		if (!udf_initialized) {
			ukf.Initialize(filter_percentage, current_time);
			udf_initialized = true;
		} else {
			ukf.Predict(current_time);
			if (percentage != last_percentage) {
				ukf.Update(filter_percentage);
			}
		}

		double estimated_seconds_remaining = ukf.GetEstimatedRemainingSeconds();
		auto percentage_int = NormalizePercentage(percentage);
		PrintProgressInternal(percentage_int, estimated_seconds_remaining);
		Printer::Flush(OutputStream::STREAM_STDOUT);
		last_update_time = current_time;
		last_percentage = percentage;
	}
}

void TerminalProgressBarDisplay::PeriodicUpdate() {
	std::unique_lock<std::mutex> lock(mtx);
	while (true) {
		const double current_time = GetElapsedDuration();

		// Only advance the time, but not the progress.
		ukf.Predict(current_time);
		double estimated_seconds_remaining = ukf.GetEstimatedRemainingSeconds();

		if (last_percentage < 100) {
			auto percentage_int = NormalizePercentage(last_percentage);
			PrintProgressInternal(percentage_int, estimated_seconds_remaining);
			Printer::Flush(OutputStream::STREAM_STDOUT);
		}

		// Wait for an interval then do an update.
		if (cv.wait_for(lock, std::chrono::milliseconds(UPDATE_INTERVAL_MS),
		                [this] { return !run_periodic_updates; })) {
			break;
		}
	}
}

void TerminalProgressBarDisplay::StopPeriodicUpdates() {
	if (run_periodic_updates) {
		{
			std::lock_guard<std::mutex> lock(mtx);
			run_periodic_updates = false;
		}
		cv.notify_one();
		if (periodic_update_thread.joinable()) {
			periodic_update_thread.join();
		}
	}
	run_periodic_updates = false;
}

void TerminalProgressBarDisplay::Finish() {
	StopPeriodicUpdates();

	std::lock_guard<std::mutex> lock(mtx);
	PrintProgressInternal(100, GetElapsedDuration(), true);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
