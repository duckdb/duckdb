#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include <sstream>

namespace duckdb {

LinesPerBoundary::LinesPerBoundary() {
}
LinesPerBoundary::LinesPerBoundary(idx_t boundary_idx_p, idx_t lines_in_batch_p)
    : boundary_idx(boundary_idx_p), lines_in_batch(lines_in_batch_p) {
}

CSVErrorHandler::CSVErrorHandler(bool ignore_errors_p) : ignore_errors(ignore_errors_p) {
}

void CSVErrorHandler::Error(CSVError &csv_error) {
	LinesPerBoundary mock;
	Error(mock, csv_error, true);
}
void CSVErrorHandler::Error(LinesPerBoundary &error_info, CSVError &csv_error, bool force_error) {
	if (ignore_errors && !force_error) {
		lock_guard<mutex> parallel_lock(main_mutex);
		// We store this error
		errors.push_back({error_info, csv_error});
		return;
	}

	std::ostringstream error;
	if (PrintLineNumber(csv_error)) {
		error << "CSV Error on Line: " << GetLine(error_info) << std::endl;
	}
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		got_borked = true;
	}
	error << csv_error.error_message;
	switch (csv_error.type) {
	case CSVErrorType::CAST_ERROR:
		throw ConversionException(error.str());
	case CSVErrorType::COLUMN_NAME_TYPE_MISMATCH:
		throw BinderException(error.str());
	case CSVErrorType::NULLPADDED_QUOTED_NEW_VALUE:
		throw ParameterNotAllowedException(error.str());
	default:
		throw InvalidInputException(error.str());
	}
}

void CSVErrorHandler::Insert(idx_t boundary_idx, idx_t rows) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (lines_per_batch_map.find(boundary_idx) == lines_per_batch_map.end()) {
		lines_per_batch_map[boundary_idx] = {boundary_idx, rows};
	} else {
		lines_per_batch_map[boundary_idx].lines_in_batch += rows;
	}
}

void CSVErrorHandler::NewMaxLineSize(idx_t scan_line_size) {
	lock_guard<mutex> parallel_lock(main_mutex);
	max_line_length = std::max(scan_line_size, max_line_length);
}

CSVError::CSVError(string error_message_p, CSVErrorType type_p)
    : error_message(std::move(error_message_p)), type(type_p) {
}

CSVError::CSVError(string error_message_p, CSVErrorType type_p, idx_t column_idx_p, vector<Value> row_p)
    : error_message(std::move(error_message_p)), type(type_p), column_idx(column_idx_p), row(std::move(row_p)) {
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

CSVError CSVError::CastError(const CSVReaderOptions &options, string &column_name, string &cast_error, idx_t column_idx,
                             vector<Value> &row) {
	std::ostringstream error;
	// Which column
	error << "Error when converting column \"" << column_name << "\"." << std::endl;
	// What was the cast error
	error << cast_error << std::endl;
	error << std::endl;
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::CAST_ERROR, column_idx, row);
}

CSVError CSVError::LineSizeError(const CSVReaderOptions &options, idx_t actual_size) {
	std::ostringstream error;
	error << "Maximum line size of " << options.maximum_line_size << " bytes exceeded. ";
	error << "Actual Size:" << actual_size << " bytes." << std::endl;
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::MAXIMUM_LINE_SIZE);
}

CSVError CSVError::SniffingError(string &file_path) {
	std::ostringstream error;
	// Which column
	error << "Error when sniffing file \"" << file_path << "\"." << std::endl;
	error << "CSV options could not be auto-detected. Consider setting parser options manually." << std::endl;
	return CSVError(error.str(), CSVErrorType::SNIFFING);
}

CSVError CSVError::NullPaddingFail(const CSVReaderOptions &options) {
	std::ostringstream error;
	error << " The parallel scanner does not support null_padding in conjunction with quoted new lines. Please "
	         "disable the parallel csv reader with parallel=false"
	      << std::endl;
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::NULLPADDED_QUOTED_NEW_VALUE);
}

CSVError CSVError::UnterminatedQuotesError(const CSVReaderOptions &options, string_t *vector_ptr,
                                           idx_t vector_line_start, idx_t current_column) {
	std::ostringstream error;
	error << "Value with unterminated quote found." << std::endl;
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
	// What were the options
	error << options.ToString();
	return CSVError(error.str(), CSVErrorType::INCORRECT_COLUMN_AMOUNT);
}

bool CSVErrorHandler::PrintLineNumber(CSVError &error) {
	switch (error.type) {
	case CSVErrorType::CAST_ERROR:
	case CSVErrorType::UNTERMINATED_QUOTES:
	case CSVErrorType::INCORRECT_COLUMN_AMOUNT:
	case CSVErrorType::MAXIMUM_LINE_SIZE:
	case CSVErrorType::NULLPADDED_QUOTED_NEW_VALUE:
		return true;
	default:
		return false;
	}
}

idx_t CSVErrorHandler::GetLine(LinesPerBoundary &error_info) {
	idx_t current_line = 1 + error_info.lines_in_batch; // We start from one, since the lines are 1-indexed
	for (idx_t boundary_idx = 0; boundary_idx < error_info.boundary_idx; boundary_idx++) {
		bool batch_done = false;
		while (!batch_done) {
			if (boundary_idx == 0) {
				// if it's the first boundary, we just return
				break;
			}
			if (lines_per_batch_map.find(boundary_idx) != lines_per_batch_map.end()) {
				batch_done = true;
				current_line += lines_per_batch_map[boundary_idx].lines_in_batch;
			}
			if (got_borked) {
				return current_line;
			}
		}
	}
	return current_line;
}

} // namespace duckdb
