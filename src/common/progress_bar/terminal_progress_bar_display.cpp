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

static string FormatETA(double seconds, bool elapsed = false) {
	// for terminal rendering purposes, we need to make sure the length is always the same
	// we pad the end with spaces if that is not the case
	// the maximum length here is "(~10.35 minutes remaining)" (26 bytes)
	// always pad to this amount
	static constexpr idx_t RENDER_SIZE = 26;
	// Desired formats:
	//   00:00:00.00 remaining
	//   unknown     remaining
	//   00:00:00.00 elapsed
	if (!elapsed && seconds > 3600 * 99) {
		// estimate larger than 99 hours remaining
		string result = "(>99 hours remaining)";
		result += string(RENDER_SIZE - result.size(), ' ');
		return result;
	}
	if (seconds < 0) {
		// Invalid or unknown ETA, skip rendering estimate
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

void TerminalProgressBarDisplay::PrintProgressInternal(int32_t percentage, double seconds, bool finished) {
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
	result += FormatETA(seconds, finished);

	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

void TerminalProgressBarDisplay::Update(double percentage) {
	const double current_time = GetElapsedDuration();

	// Filters go from 0 to 1, percentage is from 0-100
	const double filter_percentage = percentage / 100.0;
	ukf.Update(filter_percentage, current_time);

	double estimated_seconds_remaining = ukf.GetEstimatedRemainingSeconds();
	auto percentage_int = NormalizePercentage(percentage);
	PrintProgressInternal(percentage_int, estimated_seconds_remaining);
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

void TerminalProgressBarDisplay::Finish() {
	PrintProgressInternal(100, GetElapsedDuration(), true);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
