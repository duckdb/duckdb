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

//! Sniffer that detects Header, Dialect and Types of CSV Files
class CSVSniffer {
public:
	explicit CSVSniffer(CSVReaderOptions options_p, StateBuffer buffer_p, const vector<LogicalType> &requested_types_p)
	    : requested_types(requested_types_p), options(options_p), buffer(std::move(buffer_p)) {};
	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	vector<CSVReaderOptions> DetectDialect();

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
	vector<CSVStateMachine *> candidates;
	//! Original Options set
	CSVReaderOptions options;
	//! Buffer being used on sniffer
	StateBuffer buffer;
	//! Analyzes if dialect candidate is a good candidate to be considered, if so, it adds it to the candidates
	void AnalyzeDialectCandidate(CSVStateMachine &state_machine);
};

} // namespace duckdb
