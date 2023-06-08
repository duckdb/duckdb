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

//! Struct to store candidates for the CSV State, with the last position they read in the buffer
struct CSVStateCandidates {
	CSVStateCandidates(CSVStateMachine *state_p, idx_t last_pos_p, idx_t max_num_columns_p)
	    : state(state_p), last_pos(last_pos_p), max_num_columns(max_num_columns_p) {};
	CSVStateMachine *state = nullptr;
	idx_t last_pos = 0;
	idx_t max_num_columns = 0;
};

//! Sniffer that detects Header, Dialect and Types of CSV Files
class CSVSniffer {
public:
	explicit CSVSniffer(CSVReaderOptions options_p, StateBuffer buffer_p, const vector<LogicalType> &requested_types_p)
	    : requested_types(requested_types_p), options(options_p), buffer(std::move(buffer_p)) {};
	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	vector<CSVReaderOptions> DetectDialect();
	//! Resets stats so it can analyze the next chunk
	void NextChunk();

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
	CSVReaderOptions options;
	//! Buffer being used on sniffer
	StateBuffer buffer;
	//! Analyzes if dialect candidate is a good candidate to be considered, if so, it adds it to the candidates
	void AnalyzeDialectCandidate(CSVStateMachine &state_machine, idx_t buffer_start_pos = 0);
};

} // namespace duckdb
