#include "duckdb/execution/operator/csv_scanner/csv_validator.hpp"

namespace duckdb {

void ThreadLines::Insert(idx_t thread_idx, ValidatorLine line_info) {
	D_ASSERT(thread_lines.find(thread_idx) == thread_lines.end());
	thread_lines.insert({thread_idx, line_info});
}

bool ThreadLines::Validate() {
	bool initialized = false;
	idx_t last_end_pos = 0;
	for (auto &line_info : thread_lines) {
		if (!initialized) {
			// First run, we just set the initialized to true
			initialized = true;
		} else {
			if (last_end_pos < line_info.second.start_pos) {
				throw InternalException(to_string(last_end_pos) + "   " + to_string(line_info.second.start_pos));
				// Start position of next thread can't be higher than end position of previous thread + error margin.
				// Validation failed.
				return false;
			}
		}
		last_end_pos = line_info.second.end_pos;
	}
	// It's all good man.
	return true;
}

void CSVValidator::Insert(idx_t file_idx, idx_t thread_idx, ValidatorLine line_info) {
	if (per_file_thread_lines.size() <= file_idx) {
		per_file_thread_lines.resize(file_idx + 1);
	}
	per_file_thread_lines[file_idx].Insert(thread_idx, line_info);
}

bool CSVValidator::Validate() {
	for (auto &file : per_file_thread_lines) {
		if (!file.Validate()) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
