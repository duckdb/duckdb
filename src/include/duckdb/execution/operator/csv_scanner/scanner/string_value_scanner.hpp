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
#include "duckdb/common/operator/integer_cast_operator.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"

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
	                  CSVErrorHandler &error_hander, CSVIterator &iterator, bool store_line_size,
	                  shared_ptr<CSVFileScan> csv_file_scan, idx_t &lines_read);

	//! Information on the vector
	unsafe_vector<void *> vector_ptr;
	unsafe_vector<ValidityMask *> validity_mask;

	//! Variables to iterate over the CSV buffers
	idx_t last_position;
	char *buffer_ptr;
	idx_t buffer_size;

	//! CSV Options that impact the parsing
	const uint32_t number_of_columns;
	const bool null_padding;
	const bool ignore_errors;
	const char *null_str_ptr;
	const idx_t null_str_size;

	//! Internal Data Chunk used for flushing
	DataChunk parse_chunk;
	idx_t number_of_rows = 0;
	idx_t cur_col_id = 0;
	idx_t result_size;
	//! Information to properly handle errors
	CSVErrorHandler &error_handler;
	CSVIterator &iterator;
	//! Where the previous line started, used to validate the maximum_line_size option
	LinePosition previous_line_start;
	LinePosition pre_previous_line_start;
	bool store_line_size = false;
	bool added_last_line = false;
	bool quoted_new_line = false;

	unsafe_unique_array<LogicalTypeId> parse_types;
	vector<string> names;
	unordered_map<idx_t, string> cast_errors;

	shared_ptr<CSVFileScan> csv_file_scan;
	idx_t &lines_read;

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

	void HandleOverLimitRows();

	inline void AddValueToVector(const char *value_ptr, const idx_t size, bool allocate = false){
	if (size == null_str_size){
			if (((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
		bool is_null = true;
		for (idx_t i = 0; i < size; i++) {
			if (null_str_ptr[i] != value_ptr[i]) {
				is_null = false;
				break;
			}
		}
		if (is_null) {
			bool empty = false;
			if (cur_col_id < state_machine.options.force_not_null.size()) {
				empty = state_machine.options.force_not_null[cur_col_id];
			}
			if (empty) {
				if (cur_col_id >= number_of_columns) {
					HandleOverLimitRows();
				}
				if (parse_types[cur_col_id] != LogicalTypeId::VARCHAR){
					// If it is not a varchar, empty values are not accepted, we must error.
					cast_errors[cur_col_id] = std::string("");
				}
				static_cast<string_t *>(vector_ptr[cur_col_id])[number_of_rows] = string_t();
			} else {
				if (cur_col_id == number_of_columns) {
					// We check for a weird case, where we ignore an extra value, if it is a null value
					return;
				}
				validity_mask[cur_col_id]->SetInvalid(number_of_rows);
			}
			cur_col_id++;
			return;
		}
	}

	}
	if (cur_col_id >= number_of_columns) {
		HandleOverLimitRows();
	}
	bool success = true;
	switch (parse_types[cur_col_id]) {
	case LogicalTypeId::TINYINT:
		success =
		    TrySimpleIntegerCast(value_ptr, size, static_cast<int8_t *>(vector_ptr[cur_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::SMALLINT:
		success = TrySimpleIntegerCast(value_ptr, size, static_cast<int16_t *>(vector_ptr[cur_col_id])[number_of_rows],
		                               false);
		break;
	case LogicalTypeId::INTEGER:
		success = TrySimpleIntegerCast(value_ptr, size, static_cast<int32_t *>(vector_ptr[cur_col_id])[number_of_rows],
		                               false);
		break;
	case LogicalTypeId::BIGINT:
		success = TrySimpleIntegerCast(value_ptr, size, static_cast<int64_t *>(vector_ptr[cur_col_id])[number_of_rows],
		                               false);
		break;
	case LogicalTypeId::UTINYINT:
		success = TrySimpleIntegerCast<uint8_t, false>(
		    value_ptr, size, static_cast<uint8_t *>(vector_ptr[cur_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::USMALLINT:
		success = TrySimpleIntegerCast<uint16_t, false>(
		    value_ptr, size, static_cast<uint16_t *>(vector_ptr[cur_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UINTEGER:
		success = TrySimpleIntegerCast<uint32_t, false>(
		    value_ptr, size, static_cast<uint32_t *>(vector_ptr[cur_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UBIGINT:
		success = TrySimpleIntegerCast<uint64_t, false>(
		    value_ptr, size, static_cast<uint64_t *>(vector_ptr[cur_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::DOUBLE:
		success = TryDoubleCast<double>(value_ptr, size, static_cast<double *>(vector_ptr[cur_col_id])[number_of_rows],
		                                false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::FLOAT:
		success = TryDoubleCast<float>(value_ptr, size, static_cast<float *>(vector_ptr[cur_col_id])[number_of_rows],
		                               false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::DATE: {
		idx_t pos;
		bool special;
		success = Date::TryConvertDate(value_ptr, size, pos,
		                               static_cast<date_t *>(vector_ptr[cur_col_id])[number_of_rows], special, false);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		success = Timestamp::TryConvertTimestamp(value_ptr, size,
		                                         static_cast<timestamp_t *>(vector_ptr[cur_col_id])[number_of_rows]) ==
		          TimestampCastResult::SUCCESS;
		break;
	}
	default:
		if (allocate) {
			static_cast<string_t *>(vector_ptr[cur_col_id])[number_of_rows] =
			    StringVector::AddStringOrBlob(parse_chunk.data[cur_col_id], string_t(value_ptr, size));
		} else {
			static_cast<string_t *>(vector_ptr[cur_col_id])[number_of_rows] = string_t(value_ptr, size);
		}
		break;
	}
	if (!success) {
		// We had a casting error, we push it here because we can only error when finishing the line read.
		cast_errors[cur_col_id] = std::string(value_ptr, size);
	}
	cur_col_id++;
}

	Value GetValue(idx_t row_idx, idx_t col_idx);

	DataChunk &ToChunk();
};

//! Our dialect scanner basically goes over the CSV and actually parses the values to a DuckDB vector of string_t
class StringValueScanner : public BaseScanner {
public:
	StringValueScanner(idx_t scanner_idx, const shared_ptr<CSVBufferManager> &buffer_manager,
	                   const shared_ptr<CSVStateMachine> &state_machine,
	                   const shared_ptr<CSVErrorHandler> &error_handler,
	                   shared_ptr<CSVFileScan> csv_file_scan = nullptr, CSVIterator boundary = {},
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

	//! If we can directly cast the type when consuming the CSV file, or we have to do it later
	static bool CanDirectlyCast(const LogicalType &type,
	                            const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &format_options);

	const idx_t scanner_idx;

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
	unique_ptr<CSVBufferHandle> previous_buffer_handle;
};

} // namespace duckdb
