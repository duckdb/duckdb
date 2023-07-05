//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include <vector>

namespace duckdb {

//! Different Quote Rules
enum class QuoteRule : uint8_t { QUOTES_RFC = 0, QUOTES_OTHER = 1, NO_QUOTES = 2 };

//! Struct to store candidates for the CSV State, with the last position they read in the buffer
struct CSVStateCandidates {
	CSVStateCandidates(CSVStateMachine *state_p, idx_t max_num_columns_p)
	    : state(state_p), max_num_columns(max_num_columns_p) {};
	CSVStateMachine *state = nullptr;
	idx_t max_num_columns = 0;
};

//! Struct to store the result of the Sniffer
struct SnifferResult {
	//! Return Types that were detected
	vector<LogicalType> return_types;
	//! Column Names that were detected
	vector<string> names;
	//! The CSV Options that were detected (e.g., delimiter, quotes,...)
	CSVReaderOptions options;
};

//! Sniffer that detects Header, Dialect and Types of CSV Files
class CSVSniffer {
public:
	explicit CSVSniffer(CSVReaderOptions options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
	                    const vector<LogicalType> &requested_types_p = vector<LogicalType>())
	    : requested_types(requested_types_p), options(std::move(options_p)),
	      buffer_manager(std::move(buffer_manager_p)) {
		// Check if any type is BLOB
		for (auto &type : requested_types) {
			if (type.id() == LogicalTypeId::BLOB) {
				throw InvalidInputException(
				    "CSV auto-detect for blobs not supported: there may be invalid UTF-8 in the file");
			}
		}

		if (options.skip_rows_set) {
			// Skip rows if they are set
			for (idx_t i = 0; i < options.skip_rows; i++) {
				buffer_manager->file_handle->ReadLine();
			}
		}
		//! Initialize Format Candidates
		for (const auto &t : format_template_candidates) {
			best_format_candidates[t.first].clear();
		}
	};

	//! Resets stats so it can analyze the next chunk
	void ResetStats();
	//! Main method that sniffs the CSV file, returns the types, names and options as a result
	SnifferResult SniffCSV();

private:
	//! Number of rows read
	idx_t rows_read = 0;
	//! Best Number of consistent rows (i.e., presenting all columns)
	idx_t best_consistent_rows = 0;
	//! Highest number of columns found
	idx_t best_num_cols = 0;
	//! If padding was necessary (i.e., rows are missing some columns, how much)
	idx_t prev_padding_count = 0;
	//! The types requested via the CSV Options (If any)
	const vector<LogicalType> &requested_types;
	//! Vector of CSV State Machines
	vector<CSVStateMachine> csv_state_machines;
	//! Current Candidates being considered
	vector<CSVStateCandidates> candidates;
	//! Original Options set
	const CSVReaderOptions options;
	//! Buffer being used on sniffer
	shared_ptr<CSVBufferManager> buffer_manager;

	//! ------------------------------------------------------//
	//! ----------------- Dialect Detection ----------------- //
	//! ------------------------------------------------------//
	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	void DetectDialect();

	//! Variables for Dialect Detection
	//! Candidates for the delimiter
	vector<char> delim_candidates;
	//! Quote-Rule Candidates
	vector<QuoteRule> quoterule_candidates;
	//! Candidates for the quote option
	vector<vector<char>> quote_candidates_map;
	//! Candidates for the escape option
	vector<vector<char>> escape_candidates_map = {{'\0', '\"', '\''}, {'\\'}, {'\0'}};

	//! Functions called in the main DetectDialect(); function
	//! 1. Generates the search space candidates for the dialect
	void GenerateCandidateDetectionSearchSpace();
	//! 2. Generates the search space candidates for the state machines
	void GenerateStateMachineSearchSpace();
	//! 3. Analyzes if dialect candidate is a good candidate to be considered, if so, it adds it to the candidates
	void AnalyzeDialectCandidate(CSVStateMachine &state_machine, idx_t prev_column_count = 0);
	//! 4. Refine Candidates over remaining chunks
	void RefineCandidates();

	//! ------------------------------------------------------//
	//! ------------------- Type Detection ------------------ //
	//! ------------------------------------------------------//
	//! Second phase of auto detection: detect types, format template candidates
	//! ordered by descending specificity (~ from high to low)
	void DetectTypes();
	//! Change the date format for the type to the string
	//! Try to cast a string value to the specified sql type
	bool TryCastValue(const Value &value, const LogicalType &sql_type);
	void SetDateFormat(CSVStateCandidates &candidate, const string &format_specifier, const LogicalTypeId &sql_type);

	//! Variables for Type Detection
	//! Format Candidates for Date and Timestamp Types
	const std::map<LogicalTypeId, vector<const char *>> format_template_candidates = {
	    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
	    {LogicalTypeId::TIMESTAMP,
	     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
	      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S"}},
	};
	vector<vector<LogicalType>> best_sql_types_candidates;
	map<LogicalTypeId, vector<string>> best_format_candidates;
	DataChunk best_header_row;
	CSVStateCandidates *best_candidate = nullptr;
};

} // namespace duckdb
