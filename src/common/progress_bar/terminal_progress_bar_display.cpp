#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"

#include <cmath>

namespace duckdb {

int32_t TerminalProgressBarDisplay::NormalizePercentage(double percentage) {
	if (percentage > 100) {
		return 100;
	}
	if (percentage < 0) {
		return 0;
	}
	return int32_t(percentage);
}

string TerminalProgressBarDisplay::FormatETA(double seconds, bool elapsed) {
	// for terminal rendering purposes, we need to make sure the length is always the same
	// we pad the end with spaces if that is not the case
	// the maximum length here is "(~10.35 minutes remaining)" (26 bytes)
	// always pad to this amount
	static constexpr idx_t RENDER_SIZE = 26;

	if (seconds < 0 || seconds == 2147483647) {
		// Invalid or unknown ETA, skip rendering estimate
		return string(RENDER_SIZE, ' ');
	}

	if (!elapsed && seconds > 3600 * 99) {
		// estimate larger than 99 hours remaining, treat this as invalid/unknown ETA
		return string(RENDER_SIZE, ' ');
	}

	// Round to nearest centisecond as integer.
	auto total_centiseconds = static_cast<uint64_t>(std::llround(seconds * 100.0));

	// Split into seconds and centiseconds
	uint64_t total_seconds = total_centiseconds / 100;
	uint32_t centiseconds = static_cast<uint32_t>(total_centiseconds % 100);

	// Break down total_seconds
	uint64_t hours = total_seconds / 3600;
	uint32_t minutes = static_cast<uint32_t>((total_seconds % 3600) / 60);
	uint32_t secs = static_cast<uint32_t>(total_seconds % 60);
	string result;
	result = "(";
	if (!elapsed) {
		if (hours == 0 && minutes == 0 && secs == 0) {
			result += StringUtil::Format("<1 second");
		} else if (hours == 0 && minutes == 0) {
			result += StringUtil::Format("~%u second%s", secs, secs > 1 ? "s" : "");
		} else if (hours == 0) {
			auto minute_fraction = static_cast<uint32_t>(static_cast<double>(secs) / 60.0 * 10);
			result += StringUtil::Format("~%u.%u minutes", minutes, minute_fraction);
		} else {
			auto hour_fraction = static_cast<uint32_t>(static_cast<double>(minutes) / 60.0 * 10);
			result += StringUtil::Format("~%llu.%u hours", hours, hour_fraction);
		}
		result += " remaining";
	} else {
		result += StringUtil::Format("%02llu:%02u:%02u.%02llu", hours, minutes, secs, centiseconds);
		result += " elapsed";
	}
	result += ")";
	if (result.size() < RENDER_SIZE) {
		result += string(RENDER_SIZE - result.size(), ' ');
	}
	return result;
}

string TerminalProgressBarDisplay::FormatProgressBar(const ProgressBarDisplayInfo &display, int32_t percentage) {
	// we divide the number of blocks by the percentage
	// 0%   = 0
	// 100% = PROGRESS_BAR_WIDTH
	// the percentage determines how many blocks we need to draw
	double blocks_to_draw = static_cast<double>(display.width) * (percentage / 100.0);
	// because of the power of unicode, we can also draw partial blocks
	string result;
	result += display.progress_start;
	idx_t i;
	for (i = 0; i < idx_t(blocks_to_draw); i++) {
		result += display.progress_block;
	}
	if (i < display.width) {
		// print a partial block based on the percentage of the progress bar remaining
		idx_t index = idx_t((blocks_to_draw - static_cast<double>(idx_t(blocks_to_draw))) *
		                    static_cast<double>(display.partial_block_count));
		if (index >= display.partial_block_count) {
			index = display.partial_block_count - 1;
		}
		result += display.progress_partial[index];
		i++;
	}
	for (; i < display.width; i++) {
		result += display.progress_empty;
	}
	result += display.progress_end;
	return result;
}

double TerminalProgressBarDisplay::EstimateRemainingSeconds(double percentage, double elapsed_seconds,
                                                            double observed_progress_per_second) {
	if (percentage <= 0 || elapsed_seconds <= 0) {
		return 2147483647.0;
	}
	if (percentage >= 100) {
		return 0.0;
	}
	// The elapsed/progress estimate is stable even when progress updates are sparse.
	auto cumulative_estimate = elapsed_seconds * ((100.0 - percentage) / percentage);
	if (observed_progress_per_second <= 0) {
		return cumulative_estimate;
	}
	auto observed_estimate = (100.0 - percentage) / observed_progress_per_second;
	// Short-window rates can be noisy, so only let them correct the cumulative baseline within a bounded range.
	auto minimum_estimate = cumulative_estimate * 0.5;
	auto maximum_estimate = cumulative_estimate * 2.0;
	return MaxValue<double>(minimum_estimate, MinValue<double>(maximum_estimate, observed_estimate));
}

void TerminalProgressBarDisplay::PrintProgressInternal(int32_t percentage, double seconds, bool finished) {
	string result;

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
	result += FormatProgressBar(display_info, percentage);
	result += " ";
	result += FormatETA(seconds, finished);

	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

void TerminalProgressBarDisplay::Update(double percentage) {
	const double current_time = GetElapsedDuration();

	auto estimated_seconds_remaining =
	    std::min(UpdateEstimatedRemainingSeconds(percentage, current_time), 2147483647.0);
	auto percentage_int = NormalizePercentage(percentage);

	TerminalProgressBarDisplayedProgressInfo updated_progress_info = {(idx_t)percentage_int,
	                                                                  (idx_t)estimated_seconds_remaining};
	if (displayed_progress_info != updated_progress_info) {
		PrintProgressInternal(percentage_int, estimated_seconds_remaining);
		Printer::Flush(OutputStream::STREAM_STDOUT);
		displayed_progress_info = updated_progress_info;
	}
}

double TerminalProgressBarDisplay::UpdateEstimatedRemainingSeconds(double percentage, double elapsed_seconds) {
	if (percentage <= 0 || elapsed_seconds <= 0 || percentage >= 100) {
		return EstimateRemainingSeconds(percentage, elapsed_seconds);
	}
	auto cumulative_estimate = EstimateRemainingSeconds(percentage, elapsed_seconds);
	if (!has_eta_sample || percentage < last_eta_percentage || elapsed_seconds <= last_eta_update_time) {
		// Progress may restart or move backwards when the display is reused for another operation.
		has_eta_sample = true;
		last_eta_percentage = percentage;
		last_eta_sample_time = elapsed_seconds;
		last_eta_update_time = elapsed_seconds;
		smoothed_progress_per_second = 0.0;
		estimated_completion_time = elapsed_seconds + cumulative_estimate;
		return cumulative_estimate;
	}
	if (percentage > last_eta_percentage) {
		auto progress_delta = percentage - last_eta_percentage;
		auto elapsed_delta = elapsed_seconds - last_eta_sample_time;
		auto observed_progress_per_second = progress_delta / elapsed_delta;
		static constexpr double ETA_SMOOTHING_FACTOR = 0.3;
		if (smoothed_progress_per_second <= 0) {
			smoothed_progress_per_second = observed_progress_per_second;
		} else {
			smoothed_progress_per_second = ETA_SMOOTHING_FACTOR * observed_progress_per_second +
			                               (1.0 - ETA_SMOOTHING_FACTOR) * smoothed_progress_per_second;
		}
		last_eta_percentage = percentage;
		last_eta_sample_time = elapsed_seconds;
	}
	auto candidate_remaining = EstimateRemainingSeconds(percentage, elapsed_seconds, smoothed_progress_per_second);
	auto candidate_completion_time = elapsed_seconds + candidate_remaining;
	// Smooth the completion time, not the remaining time, so the ETA naturally ticks down between samples.
	if (candidate_completion_time < estimated_completion_time) {
		static constexpr double ETA_DEADLINE_SMOOTHING_FACTOR = 0.3;
		estimated_completion_time = ETA_DEADLINE_SMOOTHING_FACTOR * candidate_completion_time +
		                            (1.0 - ETA_DEADLINE_SMOOTHING_FACTOR) * estimated_completion_time;
	} else {
		auto elapsed_delta = elapsed_seconds - last_eta_update_time;
		// Avoid large upward ETA jumps; let a later estimate move out at most at wall-clock speed.
		estimated_completion_time =
		    MinValue<double>(candidate_completion_time, estimated_completion_time + elapsed_delta);
	}
	last_eta_update_time = elapsed_seconds;
	return MaxValue<double>(estimated_completion_time - elapsed_seconds, 0.0);
}

void TerminalProgressBarDisplay::Finish() {
	PrintProgressInternal(100, GetElapsedDuration(), true);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
