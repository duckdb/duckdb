//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/operator/csv_scanner/quote_rules.hpp"
#include "duckdb/execution/operator/csv_scanner/column_count_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_schema.hpp"
#include "duckdb/execution/operator/csv_scanner/header_value.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/sniff_result.hpp"

namespace duckdb {
struct DateTimestampSniffing {
	bool initialized = false;
	bool had_match = false;
	vector<string> format;
	idx_t initial_size;
};

//! All the options that will be used to sniff the dialect of the CSV file
struct DialectCandidates {
	//! The constructor populates all of our the options that will be used in our sniffer search space
	explicit DialectCandidates(const CSVStateMachineOptions &options);

	//! Static functions to get defaults of the search space
	static vector<string> GetDefaultDelimiter();

	static vector<vector<char>> GetDefaultQuote();

	static vector<QuoteRule> GetDefaultQuoteRule();

	static vector<vector<char>> GetDefaultEscape();

	static vector<char> GetDefaultComment();

	string Print();

	//! Candidates for the delimiter
	vector<string> delim_candidates;
	//! Candidates for the comment
	vector<char> comment_candidates;
	//! Quote-Rule Candidates
	vector<QuoteRule> quote_rule_candidates;
	//! Candidates for the quote option
	unordered_map<uint8_t, vector<char>> quote_candidates_map;
	//! Candidates for the escape option
	unordered_map<uint8_t, vector<char>> escape_candidates_map;
};

//! This represents the data related to columns that have been set by the user
//! e.g., from a copy command
struct SetColumns {
	SetColumns(const vector<LogicalType> *types_p, const vector<string> *names_p) : types(types_p), names(names_p) {
		if (!types) {
			D_ASSERT(!types && !names);
		} else {
			D_ASSERT(types->size() == names->size());
		}
	}
	SetColumns() {};
	//! Return Types that were detected
	const vector<LogicalType> *types = nullptr;
	//! Column Names that were detected
	const vector<string> *names = nullptr;
	//! If columns are set
	bool IsSet() const;
	//! How many columns
	idx_t Size() const;
	//! Helper function that checks if candidate is acceptable based on the number of columns it produces
	inline bool IsCandidateUnacceptable(const idx_t num_cols, bool null_padding, bool ignore_errors,
	                                    bool last_value_always_empty) const {
		if (!IsSet() || ignore_errors) {
			// We can't say its unacceptable if it's not set or if we ignore errors
			return false;
		}
		idx_t size = Size();
		// If the columns are set and there is a mismatch with the expected number of columns, with null_padding and
		// ignore_errors not set, we don't have a suitable candidate.
		// Note that we compare with max_columns_found + 1, because some broken files have the behaviour where two
		// columns are represented as: | col 1 | col_2 |
		if (num_cols == size || num_cols == size + last_value_always_empty) {
			// Good Candidate
			return false;
		}
		// if we detected more columns than we have set, it's all good because we can null-pad them
		if (null_padding && num_cols > size) {
			return false;
		}

		// Unacceptable
		return true;
	}
};

//! Struct used to know if we have a date or timestamp type already identified in this CSV File
struct HasType {
	bool date = false;
	bool timestamp = false;
};

//! Sniffer that detects Header, Dialect and Types of CSV Files
class CSVSniffer {
public:
	explicit CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
	                    CSVStateMachineCache &state_machine_cache, bool default_null_to_varchar = true);

	//! Main method that sniffs the CSV file, returns the types, names and options as a result
	//! CSV Sniffing consists of five steps:
	//! 1. Dialect Detection: Generate the CSV Options (delimiter, quote, escape, etc.)
	//! 2. Type Detection: Figures out the types of the columns (For one chunk)
	//! 3. Type Refinement: Refines the types of the columns for the remaining chunks
	//! 4. Header Detection: Figures out if  the CSV file has a header and produces the names of the columns
	//! 5. Type Replacement: Replaces the types of the columns if the user specified them
	SnifferResult SniffCSV(bool force_match = false);

	//! I call it adaptive, since that's a sexier term.
	//! In practice this Function that only sniffs the first two rows, to verify if a header exists and what are the
	//! data types It does this considering a priorly set CSV schema. If there is a mismatch of the schema it runs the
	//! full on blazing all guns sniffer, if that still fails it tells the user to union_by_name.
	//! It returns the projection order.
	SnifferResult AdaptiveSniff(const CSVSchema &file_schema);

	//! Function that only sniffs the first two rows, to verify if a header exists and what are the data types
	AdaptiveSnifferResult MinimalSniff();

	static NewLineIdentifier DetectNewLineDelimiter(CSVBufferManager &buffer_manager);

	//! If a string_t value can be cast to a type
	static bool CanYouCastIt(ClientContext &context, const string_t value, const LogicalType &type,
	                         const DialectOptions &dialect_options, const bool is_null, const char decimal_separator);

	idx_t LinesSniffed() const;

	bool EmptyOrOnlyHeader() const;

private:
	//! CSV State Machine Cache
	CSVStateMachineCache &state_machine_cache;
	//! Highest number of columns found
	idx_t max_columns_found = 0;
	//! Current Candidates being considered
	vector<unique_ptr<ColumnCountScanner>> candidates;
	//! Reference to original CSV Options, it will be modified as a result of the sniffer.
	CSVReaderOptions &options;
	//! Buffer being used on sniffer
	shared_ptr<CSVBufferManager> buffer_manager;
	//! Information regarding columns that were set by user/query
	SetColumns set_columns;
	shared_ptr<CSVErrorHandler> error_handler;
	shared_ptr<CSVErrorHandler> detection_error_handler;
	//! Number of lines sniffed in this sniffer
	idx_t lines_sniffed;
	//! Sets the result options
	void SetResultOptions() const;

	//! ------------------------------------------------------//
	//! ----------------- Dialect Detection ----------------- //
	//! ------------------------------------------------------//
	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	void DetectDialect();
	//! Functions called in the main DetectDialect(); function
	//! 1. Generates the search space candidates for the state machines
	void GenerateStateMachineSearchSpace(vector<unique_ptr<ColumnCountScanner>> &column_count_scanners,
	                                     const DialectCandidates &dialect_candidates);

	//! 2. Analyzes if dialect candidate is a good candidate to be considered, if so, it adds it to the candidates
	void AnalyzeDialectCandidate(unique_ptr<ColumnCountScanner>, idx_t &rows_read, idx_t &best_consistent_rows,
	                             idx_t &prev_padding_count, idx_t &min_ignored_rows);
	//! 3. Refine Candidates over remaining chunks
	void RefineCandidates();

	//! Checks if candidate still produces good values for the next chunk
	bool RefineCandidateNextChunk(ColumnCountScanner &candidate) const;

	//! ------------------------------------------------------//
	//! ------------------- Type Detection ------------------ //
	//! ------------------------------------------------------//
	//! Second phase of auto-detection: detect types, format template candidates
	//! ordered by descending specificity (~ from high to low)
	void DetectTypes();
	//! Change the date format for the type to the string
	//! Try to cast a string value to the specified sql type
	static void SetDateFormat(CSVStateMachine &candidate, const string &format_specifier,
	                          const LogicalTypeId &sql_type);

	//! Function that initialized the necessary variables used for date and timestamp detection
	void InitializeDateAndTimeStampDetection(CSVStateMachine &candidate, const string &separator,
	                                         const LogicalType &sql_type);
	//! Sets user defined date and time formats (if any)
	void SetUserDefinedDateTimeFormat(CSVStateMachine &candidate) const;
	//! Functions that performs detection for date and timestamp formats
	void DetectDateAndTimeStampFormats(CSVStateMachine &candidate, const LogicalType &sql_type, const string &separator,
	                                   const string_t &dummy_val);
	//! Sniffs the types from a data chunk
	void SniffTypes(DataChunk &data_chunk, CSVStateMachine &state_machine,
	                unordered_map<idx_t, vector<LogicalType>> &info_sql_types_candidates, idx_t start_idx_detection);

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
	unique_ptr<StringValueScanner> best_candidate;
	vector<HeaderValue> best_header_row;
	//! Variable used for sniffing date and timestamp
	map<LogicalTypeId, DateTimestampSniffing> format_candidates;
	map<LogicalTypeId, DateTimestampSniffing> original_format_candidates;

	//! ------------------------------------------------------//
	//! ------------------ Type Refinement ------------------ //
	//! ------------------------------------------------------//
	void RefineTypes();
	bool TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) const;
	vector<LogicalType> detected_types;
	//! If when finding a SQLNULL type in type detection we default it to varchar
	const bool default_null_to_varchar;
	//! ------------------------------------------------------//
	//! ------------------ Header Detection ----------------- //
	//! ------------------------------------------------------//
	void DetectHeader();
	static bool DetectHeaderWithSetColumn(ClientContext &context, vector<HeaderValue> &best_header_row,
	                                      const SetColumns &set_columns, CSVReaderOptions &options);
	static vector<string>
	DetectHeaderInternal(ClientContext &context, vector<HeaderValue> &best_header_row, CSVStateMachine &state_machine,
	                     const SetColumns &set_columns,
	                     unordered_map<idx_t, vector<LogicalType>> &best_sql_types_candidates_per_column_idx,
	                     CSVReaderOptions &options, CSVErrorHandler &error_handler);
	vector<string> names;
	//! If the file only has a header
	bool single_row_file = false;

	//! ------------------------------------------------------//
	//! ------------------ Type Replacement ----------------- //
	//! ------------------------------------------------------//
	void ReplaceTypes();
	vector<bool> manually_set;
};

} // namespace duckdb
