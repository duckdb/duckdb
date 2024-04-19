//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/string_value_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

namespace duckdb {

struct CSVBufferUsage {
	CSVBufferUsage(CSVBufferManager &buffer_manager_p, idx_t buffer_idx_p)
	    : buffer_manager(buffer_manager_p), buffer_idx(buffer_idx_p) {

	                                        };
	~CSVBufferUsage() {
		buffer_manager.ResetBuffer(buffer_idx);
	}
	CSVBufferManager &buffer_manager;
	idx_t buffer_idx;
};

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

	bool operator==(const LinePosition &other) const {
		return buffer_pos == other.buffer_pos && buffer_idx == other.buffer_idx && buffer_size == other.buffer_size;
	}

	idx_t GetGlobalPosition(idx_t requested_buffer_size, bool first_char_nl = false) {
		return requested_buffer_size * buffer_idx + buffer_pos + first_char_nl;
	}
	idx_t buffer_pos = 0;
	idx_t buffer_size = 0;
	idx_t buffer_idx = 0;
};

//! Keeps track of start and end of line positions in regard to the CSV file
class FullLinePosition {
public:
	FullLinePosition() {};
	LinePosition begin;
	LinePosition end;

	//! Reconstructs the current line to be used in error messages
	string ReconstructCurrentLine(bool &first_char_nl,
	                              unordered_map<idx_t, shared_ptr<CSVBufferHandle>> &buffer_handles);
};

class CurrentError {
public:
	CurrentError(CSVErrorType type, idx_t col_idx_p, LinePosition error_position_p)
	    : type(type), col_idx(col_idx_p), error_position(error_position_p) {};

	CSVErrorType type;
	idx_t col_idx;
	idx_t current_line_size;
	string error_message;
	//! Exact Position where the error happened
	LinePosition error_position;

	friend bool operator==(const CurrentError &error, CSVErrorType other) {
		return error.type == other;
	}
};

class StringValueResult : public ScannerResult {
public:
	StringValueResult(CSVStates &states, CSVStateMachine &state_machine,
	                  const shared_ptr<CSVBufferHandle> &buffer_handle, Allocator &buffer_allocator, idx_t result_size,
	                  idx_t buffer_position, CSVErrorHandler &error_handler, CSVIterator &iterator,
	                  bool store_line_size, shared_ptr<CSVFileScan> csv_file_scan, idx_t &lines_read, bool sniffing);

	~StringValueResult();

	//! Information on the vector
	unsafe_vector<void *> vector_ptr;
	unsafe_vector<ValidityMask *> validity_mask;

	//! Variables to iterate over the CSV buffers
	LinePosition last_position;
	char *buffer_ptr;
	idx_t buffer_size;

	//! CSV Options that impact the parsing
	const uint32_t number_of_columns;
	const bool null_padding;
	const bool ignore_errors;

	unsafe_unique_array<const char *> null_str_ptr;
	unsafe_unique_array<idx_t> null_str_size;
	idx_t null_str_count;

	//! Internal Data Chunk used for flushing
	DataChunk parse_chunk;
	idx_t number_of_rows = 0;
	idx_t cur_col_id = 0;
	idx_t result_size;
	//! Information to properly handle errors
	CSVErrorHandler &error_handler;
	CSVIterator &iterator;
	//! Line position of the current line
	FullLinePosition current_line_position;
	//! Used for CSV line reconstruction on flushed errors
	unordered_map<idx_t, FullLinePosition> line_positions_per_row;
	bool store_line_size = false;
	bool added_last_line = false;
	bool quoted_new_line = false;

	unsafe_unique_array<std::pair<LogicalTypeId, bool>> parse_types;
	vector<string> names;

	shared_ptr<CSVFileScan> csv_file_scan;
	idx_t &lines_read;
	//! Information regarding projected columns
	unsafe_unique_array<bool> projected_columns;
	bool projecting_columns = false;
	idx_t chunk_col_id = 0;

	//! We must ensure that we keep the buffers alive until processing the query result
	unordered_map<idx_t, shared_ptr<CSVBufferHandle>> buffer_handles;

	//! Requested size of buffers (i.e., either 32Mb or set by buffer_size parameter)
	idx_t requested_size;

	//! Errors happening in the current line (if any)
	vector<CurrentError> current_errors;
	StrpTimeFormat date_format, timestamp_format;
	bool sniffing;
	//! Specialized code for quoted values, makes sure to remove quotes and escapes
	static inline void AddQuotedValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Value to the result
	static inline void AddValue(StringValueResult &result, const idx_t buffer_pos);
	//! Adds a Row to the result
	static inline bool AddRow(StringValueResult &result, const idx_t buffer_pos);
	//! Behavior when hitting an invalid state
	static inline void InvalidState(StringValueResult &result);
	//! Handles QuotedNewline State
	static inline void QuotedNewLine(StringValueResult &result);
	void NullPaddingQuotedNewlineCheck();
	//! Handles EmptyLine states
	static inline bool EmptyLine(StringValueResult &result, const idx_t buffer_pos);
	inline bool AddRowInternal();
	//! Force the throw of a unicode error
	void HandleUnicodeError(idx_t col_idx, LinePosition &error_position);
	//! Certain errors should only be handled when adding the line, to ensure proper error propagation.
	bool HandleError();

	inline void AddValueToVector(const char *value_ptr, const idx_t size, bool allocate = false);

	Value GetValue(idx_t row_idx, idx_t col_idx);

	DataChunk &ToChunk();
	//! Resets the state of the result
	void Reset();
};

//! Our dialect scanner basically goes over the CSV and actually parses the values to a DuckDB vector of string_t
class StringValueScanner : public BaseScanner {
public:
	StringValueScanner(idx_t scanner_idx, const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler, const shared_ptr<CSVFileScan> &csv_file_scan,
	                   bool sniffing = false, CSVIterator boundary = {}, idx_t result_size = STANDARD_VECTOR_SIZE);

	StringValueScanner(const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler);

	StringValueResult &ParseChunk() override;

	//! Flushes the result to the insert_chunk
	void Flush(DataChunk &insert_chunk);

	//! Function that creates and returns a non-boundary CSV Scanner, can be used for internal csv reading.
	static unique_ptr<StringValueScanner> GetCSVScanner(ClientContext &context, CSVReaderOptions &options);

	bool FinishedIterator();

	//! Creates a new string with all escaped values removed
	static string_t RemoveEscape(const char *str_ptr, idx_t end, char escape, Vector &vector);

	//! If we can directly cast the type when consuming the CSV file, or we have to do it later
	static bool CanDirectlyCast(const LogicalType &type,
	                            const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &format_options);

	const idx_t scanner_idx;

	//! Variable that manages buffer tracking
	shared_ptr<CSVBufferUsage> buffer_tracker;

private:
	void Initialize() override;

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
	vector<LogicalType> types;

	//! Pointer to the previous buffer handle, necessary for overbuffer values
	shared_ptr<CSVBufferHandle> previous_buffer_handle;
};

} // namespace duckdb
