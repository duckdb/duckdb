//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

namespace duckdb {

//! Class that keeps track of line starts, used for line size verification
class LinePosition {
public:
	LinePosition() {
	}
	LinePosition(idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t buffer_size_p)
	    : buffer_pos(buffer_pos_p), buffer_size(buffer_size_p), buffer_idx(buffer_idx_p) {
	}

	idx_t operator-(const LinePosition &other) {
		if (other.buffer_idx == buffer_idx) {
			return buffer_pos - other.buffer_pos;
		}
		return other.buffer_size - other.buffer_pos + buffer_pos;
	}
	idx_t buffer_pos = 0;
	idx_t buffer_size = 0;
	idx_t buffer_idx = 0;
};

class StringValueResult : public ScannerResult {
public:
	StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle,
	                  Allocator &buffer_allocator, idx_t result_size, idx_t buffer_position,
	                  CSVErrorHandler &error_hander, CSVIterator &iterator, bool store_line_size);

	//! Information on the vector
	unique_ptr<Vector> vector;
	string_t *vector_ptr;
	ValidityMask *validity_mask;
	idx_t vector_size;

	//! Variables to iterate over the CSV buffers
	idx_t last_position;
	char *buffer_ptr;
	idx_t buffer_size;

	//! CSV Options that impact the parsing
	const uint32_t number_of_columns;
	const bool null_padding;
	const bool ignore_errors;
	const string_t null_str;

	//! Internal Data Chunk used for flushing
	DataChunk parse_chunk;

	idx_t result_size;

	//! Information to properly handle errors
	CSVErrorHandler &error_handler;
	CSVIterator &iterator;
	//! Where the previous line started, used to validate the maximum_line_size option
	LinePosition previous_line_start;
	LinePosition pre_previous_line_start;
	bool store_line_size = false;
	bool added_last_line = false;
	//! Last result position where a new row started
	idx_t last_row_pos = 0;
	//! Specialized code for quoted values, makes sure to remove quotes and escapes
	static inline void AddQuotedValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Value to the result
	static inline void AddValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(StringValueResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void InvalidState(StringValueResult &result);

	//! Handles EmptyLine states
	static inline bool EmptyLine(StringValueResult &result, const idx_t buffer_pos);
	inline bool AddRowInternal();

	void HandleOverLimitRows();
	void AddValueToVector(string_t &value, bool allocate = false);

	Value GetValue(idx_t row_idx, idx_t col_idx);

	DataChunk &ToChunk();

	idx_t NumberOfRows();

	void Print();
};

//! Our dialect scanner basically goes over the CSV and actually parses the values to a DuckDB vector of string_t
class StringValueScanner : public BaseScanner {
public:
	StringValueScanner(idx_t scanner_idx, const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler, CSVIterator boundary = {},
	                   idx_t result_size = STANDARD_VECTOR_SIZE);

	~StringValueScanner() {
	}

	StringValueResult &ParseChunk() override;

	//! Flushes the result to the insert_chunk
	void Flush(DataChunk &insert_chunk);

	//! Function that creates and returns a non-boundary CSV Scanner, can be used for internal csv reading.
	static unique_ptr<StringValueScanner> GetCSVScanner(ClientContext &context, CSVReaderOptions &options);

	bool FinishedIterator();

	//! Creates a new string with all escaped values removed
	static string_t RemoveEscape(const char *str_ptr, idx_t end, char escape, Vector &vector);

	const idx_t scanner_idx;

private:
	void Initialize() override;

	void Process() override;

	void FinalizeChunkProcess() override;

	//! Function used to process values that go over the first buffer, extra allocation might be necessary
	void ProcessOverbufferValue();

	void ProcessExtraRow();
	//! Function used to move from one buffer to the other, if necessary
	bool MoveToNextBuffer();

	//! BOM skipping (https://en.wikipedia.org/wiki/Byte_order_mark)
	void SkipBOM();

	//! Skips Notes, notes are dirty lines on top of the file, before the actual data
	void SkipCSVRows();

	void SkipUntilNewLine();

	void SetStart();

	StringValueResult result;

	//! Pointer to the previous buffer handle, necessary for overbuffer values
	unique_ptr<CSVBufferHandle> previous_buffer_handle;
};

} // namespace duckdb
