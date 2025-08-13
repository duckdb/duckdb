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

static std::string format_eta(double seconds) {
	// Round to nearest whole second
	uint64_t total_seconds = static_cast<uint64_t>(std::round(seconds));

	uint64_t hours = total_seconds / 3600;
	uint32_t minutes = (total_seconds % 3600) / 60;
	uint32_t secs = total_seconds % 60;

	return StringUtil::Format("%lu:%02d:%02d", hours, minutes, secs) + " remaining";
}

void TerminalProgressBarDisplay::PrintProgressInternal(int32_t percentage, double est_remaining) {
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
	result += " (";
	result += format_eta(est_remaining) + ")";
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
	double current_time = GetElapsedDuration();
	if (!udf_initialized) {
		ukf.Initialize(percentage / 100.0, current_time);
		udf_initialized = true;
	} else {
		ukf.Predict(current_time);
		ukf.Update(percentage / 100.0);
	}

	double estimated_seconds_remaining = ukf.GetEstimatedRemainingSeconds();

	auto percentage_int = NormalizePercentage(percentage);
	if (percentage_int == rendered_percentage) {
		return;
	}
	PrintProgressInternal(percentage_int, estimated_seconds_remaining);
	Printer::Flush(OutputStream::STREAM_STDOUT);
	rendered_percentage = percentage_int;
}

void TerminalProgressBarDisplay::Finish() {
	PrintProgressInternal(100, 0.0);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
