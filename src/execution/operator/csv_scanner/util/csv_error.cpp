#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include <sstream>

namespace duckdb {

void CSVErrorHandler::Error(LinesPerBatch &error_info, string &error_message) {
	std::ostringstream error;
	idx_t error_line = GetLine(error_info);
	error << "CSV Error on Line: " << error_line << std::endl;
	error << error_message;
	throw InvalidInputException(error.str());
}

void CSVErrorHandler::Insert(LinesPerBatch &error_info) {
	lock_guard<mutex> parallel_lock(main_mutex);
	lines_per_batch_map[error_info.batch_info] = error_info;
}

idx_t CSVErrorHandler::GetLine(LinesPerBatch &error_info) {
	idx_t current_line = 1; // We start from one, since the lines are 1-indexed
	for (idx_t batch_idx = 0; batch_idx < error_info.batch_info.batch_idx; batch_idx++) {
		bool batch_done = false;
		while (!batch_done) {
			unique_ptr<lock_guard<mutex>> parallel_lock;
			auto batch_info = lines_per_batch_map[error_info.batch_info];
			if (batch_info.initialized) {
				batch_done = true;
				current_line += batch_info.lines_in_batch;
			}
		}
	}
	return current_line;
}

} // namespace duckdb
