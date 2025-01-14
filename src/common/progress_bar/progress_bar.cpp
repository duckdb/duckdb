#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"

namespace duckdb {

void QueryProgress::Initialize() {
	percentage = -1;
	rows_processed = 0;
	total_rows_to_process = 0;
}

void QueryProgress::Restart() {
	percentage = 0;
	rows_processed = 0;
	total_rows_to_process = 0;
}

double QueryProgress::GetPercentage() {
	return percentage;
}
uint64_t QueryProgress::GetRowsProcesseed() {
	return rows_processed;
}
uint64_t QueryProgress::GetTotalRowsToProcess() {
	return total_rows_to_process;
}

QueryProgress::QueryProgress() {
	Initialize();
}

QueryProgress &QueryProgress::operator=(const QueryProgress &other) {
	if (this != &other) {
		percentage = other.percentage.load();
		rows_processed = other.rows_processed.load();
		total_rows_to_process = other.total_rows_to_process.load();
	}
	return *this;
}

QueryProgress::QueryProgress(const QueryProgress &other) {
	percentage = other.percentage.load();
	rows_processed = other.rows_processed.load();
	total_rows_to_process = other.total_rows_to_process.load();
}

void ProgressBar::SystemOverrideCheck(ClientConfig &config) {
	if (config.system_progress_bar_disable_reason != nullptr) {
		throw InvalidInputException("Could not change the progress bar setting because: '%s'",
		                            config.system_progress_bar_disable_reason);
	}
}

unique_ptr<ProgressBarDisplay> ProgressBar::DefaultProgressBarDisplay() {
	return make_uniq<TerminalProgressBarDisplay>();
}

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after,
                         progress_bar_display_create_func_t create_display_func)
    : executor(executor), show_progress_after(show_progress_after) {
	if (create_display_func) {
		display = create_display_func();
	}
}

QueryProgress ProgressBar::GetDetailedQueryProgress() {
	return query_progress;
}

void ProgressBar::Start() {
	profiler.Start();
	query_progress.Initialize();
	supported = true;
}

bool ProgressBar::PrintEnabled() const {
	return display != nullptr;
}

bool ProgressBar::ShouldPrint(bool final) const {
	if (!PrintEnabled()) {
		// Don't print progress at all
		return false;
	}
	if (!supported) {
		return false;
	}

	double elapsed_time = -1.0;
	if (elapsed_time < 0.0) {
		elapsed_time = profiler.Elapsed();
	}

	auto sufficient_time_elapsed = elapsed_time > static_cast<double>(show_progress_after) / 1000.0;
	if (!sufficient_time_elapsed) {
		// Don't print yet
		return false;
	}
	if (final) {
		// Print the last completed bar
		return true;
	}
	return query_progress.percentage > -1;
}

void ProgressBar::Update(bool final) {
	if (!final && !supported) {
		return;
	}

	ProgressData progress;
	idx_t invalid_pipelines = executor.GetPipelinesProgress(progress);

	double new_percentage = 0.0;
	if (invalid_pipelines == 0 && progress.IsValid()) {
		if (progress.total > 1e15) {
			progress.Normalize(1e15);
		}
		query_progress.rows_processed = idx_t(progress.done);
		query_progress.total_rows_to_process = idx_t(progress.total);
		new_percentage = progress.ProgressDone() * 100;
	}

	if (!final && invalid_pipelines > 0) {
		return;
	}

	if (new_percentage > query_progress.percentage) {
		query_progress.percentage = new_percentage;
	}
	if (ShouldPrint(final)) {
		if (final) {
			FinishProgressBarPrint();
		} else {
			PrintProgress(LossyNumericCast<int>(query_progress.percentage.load()));
		}
	}
}

void ProgressBar::PrintProgress(int current_percentage_p) {
	D_ASSERT(display);
	display->Update(current_percentage_p);
}

void ProgressBar::FinishProgressBarPrint() {
	if (finished) {
		return;
	}
	D_ASSERT(display);
	display->Finish();
	finished = true;
	if (query_progress.percentage == 0) {
		query_progress.Initialize();
	}
}

} // namespace duckdb
