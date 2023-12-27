//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/parser/scanner_boundary.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/base_scanner.hpp"

namespace duckdb {

class StringValueResult : public ScannerResult {
public:
	StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle);
	//! Information on the vector
	unique_ptr<Vector> vector;
	string_t *vector_ptr;
	ValidityMask *validity_mask;
	idx_t vector_size;

	//! Variables to iterate over the CSV buffers
	idx_t last_position;
	char *buffer_ptr;

	//! CSV Options that impact the parsing
	const idx_t number_of_columns;
	const bool null_padding;
	const bool ignore_errors;

	//! If this line might have too many columns
	bool maybe_too_many_columns = false;
	//! Adds a Value to the result
	static inline void AddValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(StringValueResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void Kaput(StringValueResult &result);

	inline void AddRowInternal(idx_t buffer_pos);
	Value GetValue(idx_t row_idx, idx_t col_idx);

	void ToChunk(DataChunk &parse_chunk);

	idx_t NumberOfRows();

	void Print();

	//! Returns a DataChunk
};

//! Our dialect scanner basically goes over the CSV and actually parses the values to a DuckDB vector of string_t
class StringValueScanner : public BaseScanner {
public:
	StringValueScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                   ScannerBoundary boundary = {});

	~StringValueScanner(){}

	StringValueResult *ParseChunk() override;

	//! Function that creates and returns a non-boundary CSV Scanner, can be used for internal csv reading.
	static unique_ptr<StringValueScanner> GetCSVScanner(ClientContext &context, CSVReaderOptions &options);

private:
	void Process() override;

	void FinalizeChunkProcess() override;

	//! Function used to process values that go over the first buffer, extra allocation might be necessary
	void ProcessOverbufferValue();

	//! Function used to move from one buffer to the other, if necessary
	void MoveToNextBuffer();

	//! Skips a block of empty lines
	void SkipEmptyLines();

	//! Skips Notes, notes are dirty lines on top of the file, before the actual data
	void SkipNotes();

	//! Skips the header (i.e., the first line of the file)
	void SkipHeader();

	StringValueResult result;

	//! Pointer to the previous buffer handle, necessary for overbuffer values
	unique_ptr<CSVBufferHandle> previous_buffer_handle;
};

} // namespace duckdb
