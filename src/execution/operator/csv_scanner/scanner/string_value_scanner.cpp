#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_casting.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/operator/integer_cast_operator.hpp"
#include "duckdb/common/operator/double_cast_operator.hpp"
#include <algorithm>

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine,
                                     const shared_ptr<CSVBufferHandle> &buffer_handle, Allocator &buffer_allocator,
                                     idx_t result_size_p, idx_t buffer_position, CSVErrorHandler &error_hander_p,
                                     CSVIterator &iterator_p, bool store_line_size_p,
                                     shared_ptr<CSVFileScan> csv_file_scan_p, idx_t &lines_read_p)
    : ScannerResult(states, state_machine), number_of_columns(state_machine.dialect_options.num_cols),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors),
      null_str_ptr(state_machine.options.null_str.c_str()), null_str_size(state_machine.options.null_str.size()),
      result_size(result_size_p), error_handler(error_hander_p), iterator(iterator_p),
      store_line_size(store_line_size_p), csv_file_scan(std::move(csv_file_scan_p)), lines_read(lines_read_p) {
	// Vector information
	D_ASSERT(number_of_columns > 0);
	buffer_handles.push_back(buffer_handle);
	// Buffer Information
	buffer_ptr = buffer_handle->Ptr();
	buffer_size = buffer_handle->actual_size;
	last_position = buffer_position;

	// Current Result information
	previous_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, buffer_handle->actual_size};
	pre_previous_line_start = previous_line_start;
	// Fill out Parse Types
	vector<LogicalType> logical_types;
	parse_types = make_unsafe_uniq_array<LogicalTypeId>(number_of_columns);
	if (!csv_file_scan) {
		for (idx_t i = 0; i < number_of_columns; i++) {
			parse_types[i] = LogicalTypeId::VARCHAR;
			logical_types.emplace_back(LogicalType::VARCHAR);
			string name = "Column_" + to_string(i);
			names.emplace_back(name);
		}
	} else {
		if (csv_file_scan->file_types.size() > number_of_columns) {
			throw InvalidInputException(
			    "Mismatch between the number of columns (%d) in the CSV file and what is expected in the scanner (%d).",
			    number_of_columns, csv_file_scan->file_types.size());
		}
		for (idx_t i = 0; i < csv_file_scan->file_types.size(); i++) {
			auto &type = csv_file_scan->file_types[i];
			if (StringValueScanner::CanDirectlyCast(type, state_machine.options.dialect_options.date_format)) {
				parse_types[i] = type.id();
				logical_types.emplace_back(type);
			} else {
				parse_types[i] = LogicalTypeId::VARCHAR;
				logical_types.emplace_back(LogicalType::VARCHAR);
			}
		}
		names = csv_file_scan->names;
		if (!csv_file_scan->projected_columns.empty()) {
			projecting_columns = false;
			projected_columns = make_unsafe_uniq_array<bool>(number_of_columns);
			for (idx_t col_idx = 0; col_idx < number_of_columns; col_idx++) {
				if (csv_file_scan->projected_columns.find(col_idx) == csv_file_scan->projected_columns.end()) {
					// Column is not projected
					projecting_columns = true;
					projected_columns[col_idx] = false;
				} else {
					projected_columns[col_idx] = true;
				}
			}
		}
		if (!projecting_columns) {
			for (idx_t j = logical_types.size(); j < number_of_columns; j++) {
				// This can happen if we have sneaky null columns at the end that we wish to ignore
				parse_types[j] = LogicalTypeId::VARCHAR;
				logical_types.emplace_back(LogicalType::VARCHAR);
			}
		}
	}

	// Initialize Parse Chunk
	parse_chunk.Initialize(buffer_allocator, logical_types, result_size);
	for (auto &col : parse_chunk.data) {
		vector_ptr.push_back(FlatVector::GetData<string_t>(col));
		validity_mask.push_back(&FlatVector::Validity(col));
	}
}

inline bool IsValueNull(const char *null_str_ptr, const char *value_ptr, const idx_t size) {
	for (idx_t i = 0; i < size; i++) {
		if (null_str_ptr[i] != value_ptr[i]) {
			return false;
		}
	}
	return true;
}

void StringValueResult::AddValueToVector(const char *value_ptr, const idx_t size, bool allocate) {
	if (cur_col_id >= number_of_columns) {
		bool error = true;
		if (cur_col_id == number_of_columns && ((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
			// we make an exception if the first over-value is null
			error = !IsValueNull(null_str_ptr, value_ptr, size);
		}
		if (error) {
			HandleOverLimitRows();
		}
	}
	if (ignore_current_row) {
		return;
	}
	if (projecting_columns) {
		if (!projected_columns[cur_col_id]) {
			cur_col_id++;
			return;
		}
	}
	if (size == null_str_size) {
		if (((quoted && state_machine.options.allow_quoted_nulls) || !quoted)) {
			if (IsValueNull(null_str_ptr, value_ptr, size)) {
				bool empty = false;
				if (chunk_col_id < state_machine.options.force_not_null.size()) {
					empty = state_machine.options.force_not_null[chunk_col_id];
				}
				if (empty) {
					if (parse_types[chunk_col_id] != LogicalTypeId::VARCHAR) {
						// If it is not a varchar, empty values are not accepted, we must error.
						cast_errors[chunk_col_id] = std::string("");
					}
					static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = string_t();
				} else {
					if (chunk_col_id == number_of_columns) {
						// We check for a weird case, where we ignore an extra value, if it is a null value
						return;
					}
					validity_mask[chunk_col_id]->SetInvalid(number_of_rows);
				}
				cur_col_id++;
				chunk_col_id++;
				return;
			}
		}
	}
	bool success = true;
	switch (parse_types[chunk_col_id]) {
	case LogicalTypeId::TINYINT:
		success = TrySimpleIntegerCast(value_ptr, size, static_cast<int8_t *>(vector_ptr[chunk_col_id])[number_of_rows],
		                               false);
		break;
	case LogicalTypeId::SMALLINT:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int16_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::INTEGER:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int32_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::BIGINT:
		success = TrySimpleIntegerCast(value_ptr, size,
		                               static_cast<int64_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UTINYINT:
		success = TrySimpleIntegerCast<uint8_t, false>(
		    value_ptr, size, static_cast<uint8_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::USMALLINT:
		success = TrySimpleIntegerCast<uint16_t, false>(
		    value_ptr, size, static_cast<uint16_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UINTEGER:
		success = TrySimpleIntegerCast<uint32_t, false>(
		    value_ptr, size, static_cast<uint32_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::UBIGINT:
		success = TrySimpleIntegerCast<uint64_t, false>(
		    value_ptr, size, static_cast<uint64_t *>(vector_ptr[chunk_col_id])[number_of_rows], false);
		break;
	case LogicalTypeId::DOUBLE:
		success =
		    TryDoubleCast<double>(value_ptr, size, static_cast<double *>(vector_ptr[chunk_col_id])[number_of_rows],
		                          false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::FLOAT:
		success = TryDoubleCast<float>(value_ptr, size, static_cast<float *>(vector_ptr[chunk_col_id])[number_of_rows],
		                               false, state_machine.options.decimal_separator[0]);
		break;
	case LogicalTypeId::DATE: {
		idx_t pos;
		bool special;
		success = Date::TryConvertDate(value_ptr, size, pos,
		                               static_cast<date_t *>(vector_ptr[chunk_col_id])[number_of_rows], special, false);
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		success = Timestamp::TryConvertTimestamp(
		              value_ptr, size, static_cast<timestamp_t *>(vector_ptr[chunk_col_id])[number_of_rows]) ==
		          TimestampCastResult::SUCCESS;
		break;
	}
	default:
		if (allocate) {
			static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] =
			    StringVector::AddStringOrBlob(parse_chunk.data[chunk_col_id], string_t(value_ptr, size));
		} else {
			static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = string_t(value_ptr, size);
		}
		break;
	}
	if (!success) {
		// We had a casting error, we push it here because we can only error when finishing the line read.
		cast_errors[cur_col_id] = std::string(value_ptr, size);
	}
	cur_col_id++;
	chunk_col_id++;
}

Value StringValueResult::GetValue(idx_t row_idx, idx_t col_idx) {
	if (validity_mask[col_idx]->AllValid()) {
		return Value(static_cast<string_t *>(vector_ptr[col_idx])[row_idx]);
	} else {
		if (validity_mask[col_idx]->RowIsValid(row_idx)) {
			return Value(static_cast<string_t *>(vector_ptr[col_idx])[row_idx]);
		} else {
			return Value();
		}
	}
}
DataChunk &StringValueResult::ToChunk() {
	parse_chunk.SetCardinality(number_of_rows);
	return parse_chunk;
}

void StringValueResult::Reset() {
	if (number_of_rows == 0) {
		return;
	}
	number_of_rows = 0;
	cur_col_id = 0;
	chunk_col_id = 0;
	for (auto &v : validity_mask) {
		v->SetAllValid(result_size);
	}
	buffer_handles.clear();
}

void StringValueResult::AddQuotedValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.escaped) {
		if (result.projecting_columns) {
			if (!result.projected_columns[result.cur_col_id]) {
				result.cur_col_id++;
				result.quoted = false;
				result.escaped = false;
				return;
			}
		}
		// If it's an escaped value we have to remove all the escapes, this is not really great
		auto value = StringValueScanner::RemoveEscape(
		    result.buffer_ptr + result.quoted_position + 1, buffer_pos - result.quoted_position - 2,
		    result.state_machine.dialect_options.state_machine_options.escape.GetValue(),
		    result.parse_chunk.data[result.chunk_col_id]);
		result.AddValueToVector(value.GetData(), value.GetSize());
	} else {
		if (buffer_pos < result.last_position + 2) {
			// empty value
			auto value = string_t();
			result.AddValueToVector(value.GetData(), value.GetSize());
		} else {
			result.AddValueToVector(result.buffer_ptr + result.quoted_position + 1,
			                        buffer_pos - result.quoted_position - 2);
		}
	}
	result.quoted = false;
	result.escaped = false;
}

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position > buffer_pos) {
		return;
	}
	if (result.quoted) {
		StringValueResult::AddQuotedValue(result, buffer_pos);
	} else {
		result.AddValueToVector(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	}
	result.last_position = buffer_pos + 1;
}

void StringValueResult::HandleOverLimitRows() {
	auto csv_error =
	    CSVError::IncorrectColumnAmountError(state_machine.options, nullptr, number_of_columns, cur_col_id + 1);
	LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), number_of_rows + 1);
	error_handler.Error(lines_per_batch, csv_error);
	// If we get here we need to remove the last line
	cur_col_id = 0;
	chunk_col_id = 0;
	ignore_current_row = true;
}

void StringValueResult::QuotedNewLine(StringValueResult &result) {
	result.quoted_new_line = true;
}

void StringValueResult::NullPaddingQuotedNewlineCheck() {
	// We do some checks for null_padding correctness
	if (state_machine.options.null_padding && iterator.IsBoundarySet() && quoted_new_line && iterator.done) {
		// If we have null_padding set, we found a quoted new line, we are scanning the file in parallel and it's the
		// last row of this thread.
		auto csv_error = CSVError::NullPaddingFail(state_machine.options);
		LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), number_of_rows + 1);
		error_handler.Error(lines_per_batch, csv_error, true);
	}
}

bool StringValueResult::AddRowInternal() {
	if (ignore_current_row) {
		// An error occurred on this row, we are ignoring it and resetting our control flag
		ignore_current_row = false;
		return false;
	}
	if (!cast_errors.empty()) {
		// A wild casting error appears
		// Recreate row for rejects-table
		vector<Value> row;
		if (!state_machine.options.rejects_table_name.empty()) {
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				if (cast_errors.find(col) != cast_errors.end()) {
					row.push_back(cast_errors[col]);
				} else {
					row.push_back(parse_chunk.data[col].GetValue(number_of_rows));
				}
			}
		}
		for (auto &cast_error : cast_errors) {
			std::ostringstream error;
			// Casting Error Message
			error << "Could not convert string \"" << cast_error.second << "\" to \'"
			      << LogicalTypeIdToString(parse_types[cast_error.first]) << "\'";
			auto error_string = error.str();
			auto csv_error = CSVError::CastError(state_machine.options, names[cast_error.first], error_string,
			                                     cast_error.first, row);
			LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), lines_read - 1);
			error_handler.Error(lines_per_batch, csv_error);
		}
		// If we got here it means we are ignoring errors, hence we need to signify to our result scanner to ignore this
		// row
		// Cleanup this line and continue
		cast_errors.clear();
		cur_col_id = 0;
		chunk_col_id = 0;
		return false;
	}
	NullPaddingQuotedNewlineCheck();
	quoted_new_line = false;
	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	if (cur_col_id != number_of_columns) {
		// We have too few columns:
		if (null_padding) {
			while (cur_col_id < number_of_columns) {
				bool empty = false;
				if (cur_col_id < state_machine.options.force_not_null.size()) {
					empty = state_machine.options.force_not_null[cur_col_id];
				}
				if (projecting_columns) {
					if (!projected_columns[cur_col_id]) {
						cur_col_id++;
						continue;
					}
				}
				if (empty) {
					static_cast<string_t *>(vector_ptr[chunk_col_id])[number_of_rows] = string_t();
				} else {
					validity_mask[chunk_col_id]->SetInvalid(number_of_rows);
				}
				cur_col_id++;
				chunk_col_id++;
			}
		} else {
			// If we are not null-padding this is an error
			auto csv_error =
			    CSVError::IncorrectColumnAmountError(state_machine.options, nullptr, number_of_columns, cur_col_id);
			LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(), number_of_rows + 1);
			error_handler.Error(lines_per_batch, csv_error);
			// If we are here we ignore_errors, so we delete this line
			number_of_rows--;
		}
	}
	cur_col_id = 0;
	chunk_col_id = 0;
	number_of_rows++;
	if (number_of_rows >= result_size) {
		// We have a full chunk
		return true;
	}
	return false;
}

bool StringValueResult::AddRow(StringValueResult &result, const idx_t buffer_pos) {
	if (result.last_position <= buffer_pos) {
		LinePosition current_line_start = {result.iterator.pos.buffer_idx, result.iterator.pos.buffer_pos,
		                                   result.buffer_size};
		idx_t current_line_size = current_line_start - result.previous_line_start;
		if (result.store_line_size) {
			result.error_handler.NewMaxLineSize(current_line_size);
		}
		if (current_line_size > result.state_machine.options.maximum_line_size) {
			auto csv_error = CSVError::LineSizeError(result.state_machine.options, current_line_size);
			LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(), result.number_of_rows);
			result.error_handler.Error(lines_per_batch, csv_error, true);
		}
		result.pre_previous_line_start = result.previous_line_start;
		result.previous_line_start = current_line_start;
		// We add the value
		if (result.quoted) {
			StringValueResult::AddQuotedValue(result, buffer_pos);
		} else {
			result.AddValueToVector(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
		}
		if (result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
			if (result.states.states[1] == CSVState::RECORD_SEPARATOR) {
				// Even though this is marked as a carry on, this is a hippie mixie
				result.last_position = buffer_pos + 1;
			} else {
				result.last_position = buffer_pos + 2;
			}
		} else {
			result.last_position = buffer_pos + 1;
		}
	}

	// We add the value
	return result.AddRowInternal();
}

void StringValueResult::InvalidState(StringValueResult &result) {
	// FIXME: How do we recover from an invalid state? Can we restart the state machine and jump to the next row?
	auto csv_error = CSVError::UnterminatedQuotesError(result.state_machine.options,
	                                                   static_cast<string_t *>(result.vector_ptr[result.chunk_col_id]),
	                                                   result.number_of_rows, result.cur_col_id);
	LinesPerBoundary lines_per_batch(result.iterator.GetBoundaryIdx(), result.number_of_rows);
	result.error_handler.Error(lines_per_batch, csv_error);
}

bool StringValueResult::EmptyLine(StringValueResult &result, const idx_t buffer_pos) {
	// We care about empty lines if this is a single column csv file
	result.last_position = buffer_pos + 1;
	if (result.states.IsCarriageReturn() &&
	    result.state_machine.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position++;
	}
	if (result.number_of_columns == 1) {
		if (result.null_str_size == 0) {
			bool empty = false;
			if (!result.state_machine.options.force_not_null.empty()) {
				empty = result.state_machine.options.force_not_null[0];
			}
			if (empty) {
				static_cast<string_t *>(result.vector_ptr[0])[result.number_of_rows] = string_t();
			} else {
				result.validity_mask[0]->SetInvalid(result.number_of_rows);
			}
			result.number_of_rows++;
		}
		if (result.number_of_rows >= result.result_size) {
			// We have a full chunk
			return true;
		}
	}
	return false;
}

StringValueScanner::StringValueScanner(idx_t scanner_idx_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       const shared_ptr<CSVErrorHandler> &error_handler,
                                       const shared_ptr<CSVFileScan> &csv_file_scan, CSVIterator boundary,
                                       idx_t result_size)
    : BaseScanner(buffer_manager, state_machine, error_handler, csv_file_scan, boundary), scanner_idx(scanner_idx_p),
      result(states, *state_machine, cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), result_size,
             iterator.pos.buffer_pos, *error_handler, iterator,
             buffer_manager->context.client_data->debug_set_max_line_length, csv_file_scan, lines_read) {
}

StringValueScanner::StringValueScanner(const shared_ptr<CSVBufferManager> &buffer_manager,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       const shared_ptr<CSVErrorHandler> &error_handler)
    : BaseScanner(buffer_manager, state_machine, error_handler, nullptr, {}), scanner_idx(0),
      result(states, *state_machine, cur_buffer_handle, Allocator::DefaultAllocator(), STANDARD_VECTOR_SIZE,
             iterator.pos.buffer_pos, *error_handler, iterator,
             buffer_manager->context.client_data->debug_set_max_line_length, csv_file_scan, lines_read) {
}

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	auto state_machine = make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options,
	                                                  CSVStateMachineCache::Get(context));

	state_machine->dialect_options.num_cols = options.dialect_options.num_cols;
	state_machine->dialect_options.header = options.dialect_options.header;
	auto buffer_manager = make_shared<CSVBufferManager>(context, options, options.file_path, 0);
	auto scanner = make_uniq<StringValueScanner>(buffer_manager, state_machine, make_shared<CSVErrorHandler>());
	scanner->csv_file_scan = make_shared<CSVFileScan>(context, options.file_path, options);
	scanner->csv_file_scan->InitializeProjection();
	return scanner;
}

bool StringValueScanner::FinishedIterator() {
	return iterator.done;
}

StringValueResult &StringValueScanner::ParseChunk() {
	result.Reset();
	ParseChunkInternal(result);
	return result;
}

void StringValueScanner::Flush(DataChunk &insert_chunk) {
	auto &process_result = ParseChunk();
	// First Get Parsed Chunk
	auto &parse_chunk = process_result.ToChunk();

	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);

	// We keep track of the borked lines, in case we are ignoring errors
	unordered_set<idx_t> borked_lines;
	D_ASSERT(csv_file_scan);

	auto &reader_data = csv_file_scan->reader_data;
	// Now Do the cast-aroo
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		idx_t col_idx = c;
		idx_t result_idx = reader_data.column_mapping[c];
		if (!csv_file_scan->projection_ids.empty()) {
			result_idx = reader_data.column_mapping[csv_file_scan->projection_ids[c].second];
		}
		if (col_idx >= parse_chunk.ColumnCount()) {
			throw InvalidInputException("Mismatch between the schema of different files");
		}
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		auto &parse_type = parse_vector.GetType();
		if (type == LogicalType::VARCHAR || (type != LogicalType::VARCHAR && parse_type != LogicalType::VARCHAR)) {
			// reinterpret rather than reference
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			CastParameters parameters(false, &error_message);
			bool success;
			idx_t line_error = 0;
			bool line_error_set = true;

			if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::DATE).GetValue().Empty() &&
			    type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = CSVCast::TryCastDateVector(state_machine->options.dialect_options.date_format, parse_vector,
				                                     result_vector, parse_chunk.size(), parameters, line_error);
			} else if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::TIMESTAMP)
			                .GetValue()
			                .Empty() &&
			           type.id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = CSVCast::TryCastTimestampVector(state_machine->options.dialect_options.date_format,
				                                          parse_vector, result_vector, parse_chunk.size(), parameters);
			} else if (state_machine->options.decimal_separator != "." &&
			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
				success =
				    CSVCast::TryCastFloatingVectorCommaSeparated(state_machine->options, parse_vector, result_vector,
				                                                 parse_chunk.size(), parameters, type, line_error);
			} else if (state_machine->options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
				success = CSVCast::TryCastDecimalVectorCommaSeparated(
				    state_machine->options, parse_vector, result_vector, parse_chunk.size(), parameters, type);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(buffer_manager->context, parse_vector, result_vector,
				                                    parse_chunk.size(), &error_message);
				line_error_set = false;
			}
			if (success) {
				continue;
			}
			// An error happened, to propagate it we need to figure out the exact line where the casting failed.
			UnifiedVectorFormat inserted_column_data;
			result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
			UnifiedVectorFormat parse_column_data;
			parse_vector.ToUnifiedFormat(parse_chunk.size(), parse_column_data);
			if (!line_error_set) {
				for (; line_error < parse_chunk.size(); line_error++) {
					if (!inserted_column_data.validity.RowIsValid(line_error) &&
					    parse_column_data.validity.RowIsValid(line_error)) {
						break;
					}
				}
			}
			{
				vector<Value> row;
				for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
					row.push_back(parse_chunk.GetValue(col, line_error));
				}
				auto csv_error = CSVError::CastError(state_machine->options, csv_file_scan->names[col_idx],
				                                     error_message, col_idx, row);
				LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
				                                 lines_read - parse_chunk.size() + line_error);
				error_handler->Error(lines_per_batch, csv_error);
			}
			borked_lines.insert(line_error++);
			D_ASSERT(state_machine->options.ignore_errors);
			// We are ignoring errors. We must continue but ignoring borked rows
			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					borked_lines.insert(line_error);
					vector<Value> row;
					for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
						row.push_back(parse_chunk.GetValue(col, line_error));
					}
					auto csv_error = CSVError::CastError(state_machine->options, csv_file_scan->names[col_idx],
					                                     error_message, col_idx, row);
					LinesPerBoundary lines_per_batch(iterator.GetBoundaryIdx(),
					                                 lines_read - parse_chunk.size() + line_error);
					error_handler->Error(lines_per_batch, csv_error);
				}
			}
		}
	}
	if (!borked_lines.empty()) {
		// We must remove the borked lines from our chunk
		SelectionVector succesful_rows(parse_chunk.size() - borked_lines.size());
		idx_t sel_idx = 0;
		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			if (borked_lines.find(row_idx) == borked_lines.end()) {
				succesful_rows.set_index(sel_idx++, row_idx);
			}
		}
		// Now we slice the result
		insert_chunk.Slice(succesful_rows, sel_idx);
	}
}

void StringValueScanner::Initialize() {
	states.Initialize();

	if (result.result_size != 1 && !(sniffing && state_machine->options.null_padding &&
	                                 !state_machine->options.dialect_options.skip_rows.IsSetByUser())) {
		SetStart();
	}
	result.last_position = iterator.pos.buffer_pos;
	result.previous_line_start = {iterator.pos.buffer_idx, iterator.pos.buffer_pos, cur_buffer_handle->actual_size};

	result.pre_previous_line_start = result.previous_line_start;
}

void StringValueScanner::ProcessExtraRow() {
	result.NullPaddingQuotedNewlineCheck();
	idx_t to_pos = cur_buffer_handle->actual_size;
	while (iterator.pos.buffer_pos < to_pos) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		switch (states.states[1]) {
		case CSVState::INVALID:
			result.InvalidState(result);
			iterator.pos.buffer_pos++;
			return;
		case CSVState::RECORD_SEPARATOR:
			if (states.states[0] == CSVState::RECORD_SEPARATOR) {
				lines_read++;
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
				lines_read++;
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			}
			iterator.pos.buffer_pos++;
			break;
		case CSVState::CARRIAGE_RETURN:
			lines_read++;
			if (states.states[0] != CSVState::RECORD_SEPARATOR) {
				result.AddRow(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			} else {
				result.EmptyLine(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				return;
			}
		case CSVState::DELIMITER:
			result.AddValue(result, iterator.pos.buffer_pos);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::QUOTED:
			if (states.states[0] == CSVState::UNQUOTED) {
				result.SetEscaped(result);
			}
			result.SetQuoted(result, iterator.pos.buffer_pos);
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		case CSVState::ESCAPE:
			result.SetEscaped(result);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::STANDARD:
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		case CSVState::QUOTED_NEW_LINE:
			result.quoted_new_line = true;
			result.NullPaddingQuotedNewlineCheck();
			iterator.pos.buffer_pos++;
			break;
		default:
			iterator.pos.buffer_pos++;
			break;
		}
	}
}

string_t StringValueScanner::RemoveEscape(const char *str_ptr, idx_t end, char escape, Vector &vector) {
	// Figure out the exact size
	idx_t str_pos = 0;
	bool just_escaped = false;
	for (idx_t cur_pos = 0; cur_pos < end; cur_pos++) {
		if (str_ptr[cur_pos] == escape && !just_escaped) {
			just_escaped = true;
		} else {
			just_escaped = false;
			str_pos++;
		}
	}

	auto removed_escapes = StringVector::EmptyString(vector, str_pos);
	auto removed_escapes_ptr = removed_escapes.GetDataWriteable();
	// Allocate string and copy it
	str_pos = 0;
	just_escaped = false;
	for (idx_t cur_pos = 0; cur_pos < end; cur_pos++) {
		char c = str_ptr[cur_pos];
		if (c == escape && !just_escaped) {
			just_escaped = true;
		} else {
			just_escaped = false;
			removed_escapes_ptr[str_pos++] = c;
		}
	}
	removed_escapes.Finalize();
	return removed_escapes;
}

void StringValueScanner::ProcessOverbufferValue() {
	// Process first string
	states.Initialize();
	string overbuffer_string;
	auto previous_buffer = previous_buffer_handle->Ptr();
	if (result.last_position == previous_buffer_handle->actual_size) {
		state_machine->Transition(states, previous_buffer[result.last_position - 1]);
	}
	idx_t j = 0;
	result.quoted = false;
	for (idx_t i = result.last_position; i < previous_buffer_handle->actual_size; i++) {
		state_machine->Transition(states, previous_buffer[i]);
		if (states.EmptyLine() || states.IsCurrentNewRow()) {
			continue;
		}
		if (states.NewRow() || states.NewValue()) {
			break;
		} else {
			overbuffer_string += previous_buffer[i];
		}
		if (states.IsQuoted()) {
			result.SetQuoted(result, j);
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
		j++;
	}
	if (overbuffer_string.empty() &&
	    state_machine->dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
			iterator.pos.buffer_pos++;
		}
	}
	// second buffer
	for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		if (states.EmptyLine()) {
			if (state_machine->dialect_options.num_cols == 1) {
				break;
			} else {
				continue;
			}
		}
		if (states.NewRow() || states.NewValue()) {
			break;
		} else {
			overbuffer_string += buffer_handle_ptr[iterator.pos.buffer_pos];
		}
		if (states.IsQuoted()) {
			result.SetQuoted(result, j);
		}
		if (states.IsEscaped()) {
			result.escaped = true;
		}
		j++;
	}
	string_t value;
	if (result.quoted) {
		value = string_t(overbuffer_string.c_str() + result.quoted_position,
		                 overbuffer_string.size() - 1 - result.quoted_position);
		if (result.escaped) {
			const auto str_ptr = static_cast<const char *>(overbuffer_string.c_str() + result.quoted_position);
			value =
			    StringValueScanner::RemoveEscape(str_ptr, overbuffer_string.size() - 2,
			                                     state_machine->dialect_options.state_machine_options.escape.GetValue(),
			                                     result.parse_chunk.data[result.chunk_col_id]);
		}
	} else {
		value = string_t(overbuffer_string.c_str(), overbuffer_string.size());
	}

	if (states.EmptyLine() && state_machine->dialect_options.num_cols == 1) {
		result.EmptyLine(result, iterator.pos.buffer_pos);
	} else if (!states.IsNotSet()) {
		result.AddValueToVector(value.GetData(), value.GetSize(), true);
	}

	if (states.NewRow() && !states.IsNotSet()) {
		result.AddRowInternal();
		lines_read++;
	}

	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size && cur_buffer_handle->is_last_buffer) {
		result.added_last_line = true;
	}
	if (states.IsCarriageReturn() &&
	    state_machine->dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON) {
		result.last_position = ++iterator.pos.buffer_pos + 1;
	} else {
		result.last_position = ++iterator.pos.buffer_pos;
	}
	// Be sure to reset the quoted and escaped variables
	result.quoted = false;
	result.escaped = false;
}

bool StringValueScanner::MoveToNextBuffer() {
	if (iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
		previous_buffer_handle = cur_buffer_handle;
		cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
		result.buffer_handles.push_back(cur_buffer_handle);
		if (!cur_buffer_handle) {
			iterator.pos.buffer_idx--;
			buffer_handle_ptr = nullptr;
			// We do not care if it's a quoted new line on the last row of our file.
			result.quoted_new_line = false;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (states.EmptyLine() || states.NewRow() || result.added_last_line || states.IsCurrentNewRow() ||
			    states.IsNotSet()) {
				if (result.cur_col_id == result.number_of_columns) {
					result.number_of_rows++;
				}
				result.cur_col_id = 0;
				result.chunk_col_id = 0;
				return false;
			} else if (states.NewValue()) {
				lines_read++;
				// we add the value
				result.AddValue(result, previous_buffer_handle->actual_size);
				// And an extra empty value to represent what comes after the delimiter
				result.AddRow(result, previous_buffer_handle->actual_size);
			} else if (states.IsQuotedCurrent()) {
				// Unterminated quote
				result.InvalidState(result);
			} else {
				lines_read++;
				result.AddRow(result, previous_buffer_handle->actual_size);
			}
			return false;
		}
		iterator.pos.buffer_pos = 0;
		buffer_handle_ptr = cur_buffer_handle->Ptr();
		// Handle overbuffer value
		ProcessOverbufferValue();
		result.buffer_ptr = buffer_handle_ptr;
		result.buffer_size = cur_buffer_handle->actual_size;
		return true;
	}
	return false;
}

void StringValueScanner::SkipBOM() {
	if (cur_buffer_handle->actual_size >= 3 && result.buffer_ptr[0] == '\xEF' && result.buffer_ptr[1] == '\xBB' &&
	    result.buffer_ptr[2] == '\xBF') {
		iterator.pos.buffer_pos = 3;
	}
}

void StringValueScanner::SkipCSVRows() {
	idx_t rows_to_skip =
	    state_machine->dialect_options.skip_rows.GetValue() + state_machine->dialect_options.header.GetValue();
	if (rows_to_skip == 0) {
		return;
	}
	SkipScanner row_skipper(buffer_manager, state_machine, error_handler, rows_to_skip);
	row_skipper.ParseChunk();
	iterator.pos.buffer_pos = row_skipper.GetIteratorPosition();
	if (row_skipper.state_machine->options.dialect_options.state_machine_options.new_line ==
	        NewLineIdentifier::CARRY_ON &&
	    row_skipper.states.states[1] == CSVState::CARRIAGE_RETURN) {
		iterator.pos.buffer_pos++;
	}
	if (result.store_line_size) {
		result.error_handler.NewMaxLineSize(iterator.pos.buffer_pos);
	}
	lines_read += row_skipper.GetLinesRead();
}

void StringValueScanner::SkipUntilNewLine() {
	// Now skip until next newline
	if (state_machine->options.dialect_options.state_machine_options.new_line.GetValue() ==
	    NewLineIdentifier::CARRY_ON) {
		bool carriage_return = false;
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				carriage_return = true;
			}
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				if (carriage_return) {
					iterator.pos.buffer_pos++;
					return;
				}
			}
		}
	} else {
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n' ||
			    buffer_handle_ptr[iterator.pos.buffer_pos] == '\r') {
				iterator.pos.buffer_pos++;
				return;
			}
		}
	}
}

bool StringValueScanner::CanDirectlyCast(const LogicalType &type,
                                         const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &format_options) {

	switch (type.id()) {
		// All Integers (Except HugeInt)
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
		return true;
	case LogicalTypeId::DATE:
		// We can only internally cast YYYY-MM-DD
		if (format_options.at(LogicalTypeId::DATE).GetValue().format_specifier == "%Y-%m-%d") {
			return true;
		} else {
			return false;
		}
	case LogicalTypeId::TIMESTAMP:
		if (format_options.at(LogicalTypeId::TIMESTAMP).GetValue().format_specifier == "%Y-%m-%d %H:%M:%S") {
			return true;
		} else {
			return false;
		}
	case LogicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

void StringValueScanner::SetStart() {
	if (iterator.pos.buffer_idx == 0 && iterator.pos.buffer_pos == 0) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect, so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipBOM();
		SkipCSVRows();
		return;
	}
	// We have to look for a new line that fits our schema
	// 1. We walk until the next new line
	bool line_found;
	unique_ptr<StringValueScanner> scan_finder;
	do {
		SkipUntilNewLine();
		if (state_machine->options.null_padding) {
			// When Null Padding, we assume we start from the correct new-line
			return;
		}
		scan_finder = make_uniq<StringValueScanner>(0, buffer_manager, state_machine,
		                                            make_shared<CSVErrorHandler>(true), csv_file_scan, iterator, 1);
		auto &tuples = scan_finder->ParseChunk();
		line_found = true;
		if (tuples.number_of_rows != 1) {
			line_found = false;
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			// Or if columns don't match, this is not the correct start, we need to skip until the next new line
			if (scan_finder->previous_buffer_handle) {
				if (scan_finder->iterator.pos.buffer_pos >= scan_finder->previous_buffer_handle->actual_size &&
				    scan_finder->previous_buffer_handle->is_last_buffer) {
					iterator.pos.buffer_idx = scan_finder->iterator.pos.buffer_idx;
					iterator.pos.buffer_pos = scan_finder->iterator.pos.buffer_pos;
					result.last_position = iterator.pos.buffer_pos;
					iterator.done = scan_finder->iterator.done;
					return;
				}
			}
		}
	} while (!line_found);
	iterator.pos.buffer_idx = scan_finder->result.pre_previous_line_start.buffer_idx;
	iterator.pos.buffer_pos = scan_finder->result.pre_previous_line_start.buffer_pos;
	result.last_position = iterator.pos.buffer_pos;
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.number_of_rows >= result.result_size || iterator.done) {
		// We are done
		if (!sniffing) {
			if (csv_file_scan) {
				csv_file_scan->bytes_read += bytes_read;
				bytes_read = 0;
			}
		}
		return;
	}
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (iterator.IsBoundarySet()) {
		iterator.done = true;
		// We read until the next line or until we have nothing else to read.
		// Move to next buffer
		if (!cur_buffer_handle) {
			return;
		}
		bool moved = MoveToNextBuffer();
		if (cur_buffer_handle) {
			if (moved && result.cur_col_id < result.number_of_columns && result.cur_col_id > 0) {
				ProcessExtraRow();
			} else if (!moved) {
				ProcessExtraRow();
			}
			if (cur_buffer_handle->is_last_buffer && iterator.pos.buffer_pos >= cur_buffer_handle->actual_size) {
				MoveToNextBuffer();
			}
		}
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!FinishedFile() && result.number_of_rows < result.result_size) {
			MoveToNextBuffer();
			if (result.number_of_rows >= result.result_size) {
				return;
			}
			if (cur_buffer_handle) {
				Process(result);
			}
		}
		iterator.done = FinishedFile();
		if (result.null_padding && result.number_of_rows < STANDARD_VECTOR_SIZE) {
			while (result.chunk_col_id < result.parse_chunk.ColumnCount()) {
				result.validity_mask[result.chunk_col_id++]->SetInvalid(result.number_of_rows);
				result.cur_col_id++;
			}
			result.number_of_rows++;
		}
	}
}
} // namespace duckdb
