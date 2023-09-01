//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_sniffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/operator/scan/csv/quote_rules.hpp"

namespace duckdb {
//! Struct to store the result of the Sniffer
struct SnifferResult {
	SnifferResult(vector<LogicalType> return_types_p, vector<string> names_p)
	    : return_types(std::move(return_types_p)), names(std::move(names_p)) {
	}
	//! Return Types that were detected
	vector<LogicalType> return_types;
	//! Column Names that were detected
	vector<string> names;
};

//! Sniffer that detects Header, Dialect and Types of CSV Files
class CSVSniffer {
public:
	explicit CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
	                    CSVStateMachineCache &state_machine_cache);

	//! Main method that sniffs the CSV file, returns the types, names and options as a result
	//! CSV Sniffing consists of five steps:
	//! 1. Dialect Detection: Generate the CSV Options (delimiter, quote, escape, etc.)
	//! 2. Type Detection: Figures out the types of the columns (For one chunk)
	//! 3. Header Detection: Figures out if  the CSV file has a header and produces the names of the columns
	//! 4. Type Replacement: Replaces the types of the columns if the user specified them
	//! 5. Type Refinement: Refines the types of the columns for the remaining chunks
	SnifferResult SniffCSV();

private:
	//! CSV State Machine Cache
	CSVStateMachineCache &state_machine_cache;
	//! Highest number of columns found
	idx_t max_columns_found = 0;
	//! Current Candidates being considered
	vector<unique_ptr<CSVStateMachine>> candidates;
	//! Reference to original CSV Options, it will be modified as a result of the sniffer.
	CSVReaderOptions &options;
	//! Buffer being used on sniffer
	shared_ptr<CSVBufferManager> buffer_manager;

	//! ------------------------------------------------------//
	//! ----------------- Dialect Detection ----------------- //
	//! ------------------------------------------------------//
	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	void DetectDialect();
	//! Functions called in the main DetectDialect(); function
	//! 1. Generates the search space candidates for the dialect
	void GenerateCandidateDetectionSearchSpace(vector<char> &delim_candidates, vector<QuoteRule> &quoterule_candidates,
	                                           unordered_map<uint8_t, vector<char>> &quote_candidates_map,
	                                           unordered_map<uint8_t, vector<char>> &escape_candidates_map);
	//! 2. Generates the search space candidates for the state machines
	void GenerateStateMachineSearchSpace(vector<unique_ptr<CSVStateMachine>> &csv_state_machines,
	                                     const vector<char> &delimiter_candidates,
	                                     const vector<QuoteRule> &quoterule_candidates,
	                                     const unordered_map<uint8_t, vector<char>> &quote_candidates_map,
	                                     const unordered_map<uint8_t, vector<char>> &escape_candidates_map);
	//! 3. Analyzes if dialect candidate is a good candidate to be considered, if so, it adds it to the candidates
	void AnalyzeDialectCandidate(unique_ptr<CSVStateMachine>, idx_t &rows_read, idx_t &best_consistent_rows,
	                             idx_t &prev_padding_count);
	//! 4. Refine Candidates over remaining chunks
	void RefineCandidates();
	//! Checks if candidate still produces good values for the next chunk
	bool RefineCandidateNextChunk(CSVStateMachine &candidate);

	//! ------------------------------------------------------//
	//! ------------------- Type Detection ------------------ //
	//! ------------------------------------------------------//
	//! Second phase of auto detection: detect types, format template candidates
	//! ordered by descending specificity (~ from high to low)
	void DetectTypes();
	//! Change the date format for the type to the string
	//! Try to cast a string value to the specified sql type
	bool TryCastValue(CSVStateMachine &candidate, const Value &value, const LogicalType &sql_type);
	void SetDateFormat(CSVStateMachine &candidate, const string &format_specifier, const LogicalTypeId &sql_type);
	//! Functions that performs detection for date and timestamp formats
	void DetectDateAndTimeStampFormats(CSVStateMachine &candidate, map<LogicalTypeId, bool> &has_format_candidates,
	                                   map<LogicalTypeId, vector<string>> &format_candidates,
	                                   const LogicalType &sql_type, const string &separator, Value &dummy_val);

	//! Variables for Type Detection
	//! Format Candidates for Date and Timestamp Types
	const map<LogicalTypeId, vector<const char *>> format_template_candidates = {
	    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
	    {LogicalTypeId::TIMESTAMP,
	     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
	      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S"}},
	};
	unordered_map<idx_t, vector<LogicalType>> best_sql_types_candidates_per_column_idx;
	map<LogicalTypeId, vector<string>> best_format_candidates;
	unique_ptr<CSVStateMachine> best_candidate;
	idx_t best_start_with_header = 0;
	idx_t best_start_without_header = 0;
	vector<Value> best_header_row;

	//! ------------------------------------------------------//
	//! ------------------ Header Detection ----------------- //
	//! ------------------------------------------------------//
	void DetectHeader();
	vector<string> names;

	//! ------------------------------------------------------//
	//! ------------------ Type Replacement ----------------- //
	//! ------------------------------------------------------//
	void ReplaceTypes();

	//! ------------------------------------------------------//
	//! ------------------ Type Refinement ------------------ //
	//! ------------------------------------------------------//
	void RefineTypes();
	bool TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type);
	vector<LogicalType> detected_types;
};

} // namespace duckdb
