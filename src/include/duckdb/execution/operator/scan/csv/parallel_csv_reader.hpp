//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"

namespace duckdb {

//! CSV Reader for Parallel Reading
class ParallelCSVReader : public BaseCSVReader {
public:
	ParallelCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types, idx_t file_idx_p, unique_ptr<CSVScanner> scanner);
	~ParallelCSVReader() override {
	}

	bool finished = false;

	idx_t file_idx;

	VerificationPositions GetVerificationPositions();

	//! Position of the first read line and last read line for verification purposes
	VerificationPositions verification_positions;

	//! Actual scanner
	unique_ptr<CSVScanner> scanner;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

	idx_t GetLineError(idx_t line_error, idx_t buffer_idx, bool stop_at_first = true) override;
	void Increment(idx_t buffer_idx) override;

private:

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool Parse(DataChunk &insert_chunk, string &error_message, bool try_add_line = false);
};

} // namespace duckdb
