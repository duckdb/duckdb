#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"

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

	// Round to nearest centisecond as integer
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

	// Filters go from 0 to 1, percentage is from 0-100
	const double filter_percentage = percentage / 100.0;

	ukf.Update(filter_percentage, current_time);

	double estimated_seconds_remaining;
	//  If the query is mostly completed, there can be oscillation of estimated
	//  time to completion since the progress updates seem sparse near the very
	//  end of the query, so clamp time remaining to not oscillate with estimates
	//  that are unlikely to be correct.
	if (filter_percentage > 0.99) {
		estimated_seconds_remaining = 0.5;
	} else {
		estimated_seconds_remaining = std::min(ukf.GetEstimatedRemainingSeconds(), 2147483647.0);
	}
	auto percentage_int = NormalizePercentage(percentage);

	TerminalProgressBarDisplayedProgressInfo updated_progress_info = {(idx_t)percentage_int,
	                                                                  (idx_t)estimated_seconds_remaining};
	if (displayed_progress_info != updated_progress_info) {
		PrintProgressInternal(percentage_int, estimated_seconds_remaining);
		Printer::Flush(OutputStream::STREAM_STDOUT);
		displayed_progress_info = updated_progress_info;
	}
}

void TerminalProgressBarDisplay::Finish() {
	PrintProgressInternal(100, GetElapsedDuration(), true);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
