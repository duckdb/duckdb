#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after, bool print_progress)
    : executor(executor), show_progress_after(show_progress_after), current_percentage(-1),
      print_progress(print_progress) {
}

double ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
	profiler.Start();
	current_percentage = 0;
	supported = true;
}

void ProgressBar::Update(bool final) {
	if (!supported) {
		return;
	}
	double new_percentage;
	supported = executor.GetPipelinesProgress(new_percentage);
	if (!supported) {
		return;
	}
	auto sufficient_time_elapsed = profiler.Elapsed() > show_progress_after / 1000.0;
	if (new_percentage > current_percentage) {
		current_percentage = new_percentage;
	}
	if (supported && print_progress && sufficient_time_elapsed && current_percentage > -1 && print_progress) {
#ifndef DUCKDB_DISABLE_PRINT
		if (final) {
			FinishProgressBarPrint();
		} else {
			PrintProgress(current_percentage);
		}
#endif
	}
}

void ProgressBar::PrintProgressInternal(int percentage) {
	if (percentage > 100) {
		percentage = 100;
	}
	if (percentage < 0) {
		percentage = 0;
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
	result += PROGRESS_START;
	idx_t i;
	for (i = 0; i < idx_t(blocks_to_draw); i++) {
		result += PROGRESS_BLOCK;
	}
	if (i < PROGRESS_BAR_WIDTH) {
		// print a partial block based on the percentage of the progress bar remaining
		idx_t index = idx_t((blocks_to_draw - idx_t(blocks_to_draw)) * PARTIAL_BLOCK_COUNT);
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
void ProgressBar::PrintProgress(int percentage) {
	PrintProgressInternal(percentage);
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

void ProgressBar::FinishProgressBarPrint() {
	PrintProgressInternal(100);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb
