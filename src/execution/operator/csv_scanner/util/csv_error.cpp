#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include <sstream>

namespace duckdb {

BatchInfo::BatchInfo() : file_idx(0), batch_idx(0) {};
BatchInfo::BatchInfo(idx_t file_idx_p, idx_t batch_idx_p) : file_idx(file_idx_p), batch_idx(batch_idx_p) {};

LinesPerBatch::LinesPerBatch() : initialized(false) {};
LinesPerBatch::LinesPerBatch(idx_t file_idx_p, idx_t batch_idx_p, idx_t lines_in_batch_p)
    : batch_info(file_idx_p, batch_idx_p), lines_in_batch(lines_in_batch_p) {};

CSVErrorHandler::CSVErrorHandler(bool ignore_errors_p) : ignore_errors(ignore_errors_p) {};

void CSVErrorHandler::Error(CSVError &csv_error) {
	LinesPerBatch mock;
	Error(mock, csv_error);
}
void CSVErrorHandler::Error(LinesPerBatch &error_info, CSVError &csv_error) {
	if (ignore_errors) {
		return;
	}
	std::ostringstream error;
	if (PrintLineNumber(csv_error)) {
		error << "CSV Error on Line: " << GetLine(error_info) << std::endl;
	}
	error << csv_error.error_message;
	switch (csv_error.type) {
	case CSVErrorType::CAST_ERROR:
		throw CastException(error.str());
	case CSVErrorType::COLUMN_NAME_TYPE_MISMATCH:
		throw BinderException(error.str());
	default:
		throw InvalidInputException(error.str());
	}
}

void CSVErrorHandler::Insert(LinesPerBatch &error_info) {
	lock_guard<mutex> parallel_lock(main_mutex);
	lines_per_batch_map[error_info.batch_info] = error_info;
}

CSVError::CSVError(string error_message_p, CSVErrorType type_p) : error_message(error_message_p), type(type_p) {
}

CSVError CSVError::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		auto it = sql_types_per_column.find(names[i]);
		if (it != sql_types_per_column.end()) {
			sql_types_per_column.erase(names[i]);
			continue;
		}
	}
	if (sql_types_per_column.empty()) {
		return CSVError("", CSVErrorType::COLUMN_NAME_TYPE_MISMATCH);
	}
	string exception = "COLUMN_TYPES error: Columns with names: ";
	for (auto &col : sql_types_per_column) {
		exception += "\"" + col.first + "\",";
	}
	exception.pop_back();
	exception += " do not exist in the CSV File";
	return CSVError(exception, CSVErrorType::COLUMN_NAME_TYPE_MISMATCH);
}

CSVError CSVError::CastError(const CSVReaderOptions &options, DataChunk &parse_chunk, idx_t chunk_row,
                             string &column_name, string &cast_error) {
	std::ostringstream error;
	// Which column
	error << "Error when converting column \"" << column_name << "\"." << std::endl;
	// What was the cast error
	error << cast_error << std::endl;
	// What is the problematic CSV Line
	error << "Problematic CSV Line:" << std::endl;
	for (idx_t column_idx = 0; column_idx < parse_chunk.ColumnCount(); column_idx++) {
		// error << parse_chunk.GetValue(column_idx, chunk_row).ToString();
		if (column_idx < parse_chunk.ColumnCount() - 1) {
			// we are not in the last line, add the delimiter
			error << options.dialect_options.state_machine_options.delimiter.GetValue();
		}
	}
	error << std::endl;
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::CAST_ERROR);
}

CSVError CSVError::SniffingError(string &file_path) {
	std::ostringstream error;
	// Which column
	error << "Error when sniffing file \"" << file_path << "\"." << std::endl;
	error << "CSV options could not be auto-detected. Consider setting parser options manually." << std::endl;
	return CSVError(error.str(), CSVErrorType::SNIFFING);
}

CSVError CSVError::UnterminatedQuotesError(const CSVReaderOptions &options, string_t *vector_ptr,
                                           idx_t vector_line_start, idx_t current_column) {
	std::ostringstream error;
	// What is the problematic CSV Line
	error << "Problematic CSV Line (Up to unquoted value):" << std::endl;
	for (; vector_line_start < current_column; vector_line_start++) {
		error << vector_ptr[vector_line_start].GetString();
		if (vector_line_start < current_column - 1) {
			// we are not in the last line, add the delimiter
			error << options.dialect_options.state_machine_options.delimiter.GetValue();
		}
	}
	error << std::endl;
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::UNTERMINATED_QUOTES);
}

CSVError CSVError::IncorrectColumnAmountError(const CSVReaderOptions &options, string_t *vector_ptr,
                                              idx_t vector_line_start, idx_t actual_columns) {
	std::ostringstream error;
	// How many columns were expected and how many were found
	error << "Expected Number of Columns: " << options.dialect_options.num_cols << " Found: " << actual_columns
	      << std::endl;
	// What is the problematic CSV Line
	error << "Problematic CSV Line:" << std::endl;
	for (; vector_line_start < actual_columns; vector_line_start++) {
		error << vector_ptr[vector_line_start].GetString();
		if (vector_line_start < actual_columns - 1) {
			// we are not in the last line, add the delimiter
			error << options.dialect_options.state_machine_options.delimiter.GetValue();
		}
	}
	error << std::endl;
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::INCORRECT_COLUMN_AMOUNT);
}

bool CSVErrorHandler::PrintLineNumber(CSVError &error) {
	switch (error.type) {
	case CSVErrorType::CAST_ERROR:
	case CSVErrorType::UNTERMINATED_QUOTES:
	case CSVErrorType::INCORRECT_COLUMN_AMOUNT:
		return true;
	default:
		return false;
	}
}

idx_t CSVErrorHandler::GetLine(LinesPerBatch &error_info) {
	idx_t current_line = 1 + error_info.lines_in_batch; // We start from one, since the lines are 1-indexed
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
