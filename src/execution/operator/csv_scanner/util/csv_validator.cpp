#include "duckdb/execution/operator/csv_scanner/csv_validator.hpp"
#include <sstream>

namespace duckdb {

void ThreadLines::Insert(idx_t thread_idx, ValidatorLine line_info) {
	D_ASSERT(thread_lines.find(thread_idx) == thread_lines.end());
	thread_lines.insert({thread_idx, line_info});
}

string ThreadLines::Print() const {
	string result;
	for (auto &line : thread_lines) {
		result += "{start_pos: " + std::to_string(line.second.start_pos) +
		          ", end_pos: " + std::to_string(line.second.end_pos) + "}";
	}
	return result;
}

void ThreadLines::Verify() const {
	bool initialized = false;
	idx_t last_end_pos = 0;
	for (auto &line_info : thread_lines) {
		if (!initialized) {
			// First run, we just set the initialized to true
			initialized = true;
		} else {
			if (line_info.second.start_pos == line_info.second.end_pos) {
				last_end_pos = line_info.second.end_pos;
				continue;
			}
			if (last_end_pos + error_margin < line_info.second.start_pos ||
			    line_info.second.start_pos < last_end_pos - error_margin) {
				std::ostringstream error;
				error << "The Parallel CSV Reader currently does not support a full read on this file." << '\n';
				error << "To correctly parse this file, please run with the single threaded error (i.e., parallel = "
				         "false)"
				      << '\n';
				throw NotImplementedException(error.str());
			}
		}
		last_end_pos = line_info.second.end_pos;
	}
}

void CSVValidator::Insert(idx_t file_idx, idx_t thread_idx, ValidatorLine line_info) {
	if (per_file_thread_lines.size() <= file_idx) {
		per_file_thread_lines.resize(file_idx + 1);
	}
	per_file_thread_lines[file_idx].Insert(thread_idx, line_info);
}

void CSVValidator::Verify() const {
	for (auto &file : per_file_thread_lines) {
		file.Verify();
	}
}

string CSVValidator::Print(idx_t file_idx) const {
	return per_file_thread_lines[file_idx].Print();
}

} // namespace duckdb
