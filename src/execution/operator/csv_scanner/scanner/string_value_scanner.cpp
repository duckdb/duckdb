#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_casting.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/skip_scanner.hpp"

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle,
                                     Allocator &buffer_allocator, idx_t result_size_p, idx_t buffer_position,
                                     CSVErrorHandler &error_hander_p, CSVIterator &iterator_p)
    : ScannerResult(states, state_machine), number_of_columns(state_machine.dialect_options.num_cols),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors),
      result_size(result_size_p), error_handler(error_hander_p), iterator(iterator_p) {
	// Vector information
	D_ASSERT(number_of_columns > 0);
	vector_size = number_of_columns * result_size;
	vector = make_uniq<Vector>(LogicalType::VARCHAR, vector_size);
	vector_ptr = FlatVector::GetData<string_t>(*vector);
	validity_mask = &FlatVector::Validity(*vector);

	// Buffer Information
	buffer_ptr = buffer_handle.Ptr();
	last_position = buffer_position;

	// Current Result information
	result_position = 0;

	// Initialize Parse Chunk
	parse_chunk.Initialize(buffer_allocator, {number_of_columns, LogicalType::VARCHAR}, result_size);
}

Value StringValueResult::GetValue(idx_t row_idx, idx_t col_idx) {
	idx_t vector_idx = row_idx * number_of_columns + col_idx;
	if (validity_mask->AllValid()) {
		return Value(vector_ptr[vector_idx]);
	} else {
		if (validity_mask->RowIsValid(vector_idx)) {
			return Value(vector_ptr[vector_idx]);
		} else {
			return Value();
		}
	}
}
DataChunk &StringValueResult::ToChunk() {
	idx_t number_of_rows = NumberOfRows();
	parse_chunk.Reset();
	const auto &selection_vectors = state_machine.GetSelectionVector();
	for (idx_t col_idx = 0; col_idx < parse_chunk.ColumnCount(); col_idx++) {
		// fixme: has to do some extra checks for null padding
		parse_chunk.data[col_idx].Slice(*vector, selection_vectors[col_idx], number_of_rows);
	}
	parse_chunk.SetCardinality(number_of_rows);
	return parse_chunk;
}

void StringValueResult::AddQuotedValue(StringValueResult &result, const idx_t buffer_pos) {
	if (result.escaped) {
		// If it's an escaped value we have to remove all the escapes, this is not really great
		string removed_escapes;
		char escape = result.state_machine.dialect_options.state_machine_options.escape.GetValue();
		bool just_escaped = false;
		char *str_ptr = result.buffer_ptr + result.last_position + 1;
		for (idx_t cur_pos = 0; cur_pos <= buffer_pos - result.last_position - 3; cur_pos++) {
			char c = str_ptr[cur_pos];
			if (c == escape && !just_escaped) {
				just_escaped = true;
			} else {
				just_escaped = false;
				removed_escapes += c;
			}
		}
		result.vector_ptr[result.result_position++] =
		    StringVector::AddStringOrBlob(*result.vector, string_t(removed_escapes));
	} else {
		result.vector_ptr[result.result_position++] =
		    string_t(result.buffer_ptr + result.last_position + 1, buffer_pos - result.last_position - 2);
	}
	result.quoted = false;
	result.escaped = false;
}

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
	D_ASSERT(result.result_position < result.vector_size);
	if (result.quoted) {
		StringValueResult::AddQuotedValue(result, buffer_pos - 1);
	} else {
		// Test against null value first
		auto value = string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position - 1);
		if (value == result.state_machine.options.null_str) {
			result.validity_mask->SetInvalid(result.result_position++);
		} else {
			result.vector_ptr[result.result_position++] = value;
		}
	}
	result.last_position = buffer_pos;
	if (result.result_position % result.number_of_columns == 0) {
		// This means this value reached the number of columns in a line. This is fine, if this is the last buffer, and
		// the last buffer position However, if that's not the case, this means we might be reading too many columns.
		result.maybe_too_many_columns = true;
	}
}

inline void StringValueResult::AddRowInternal(idx_t buffer_pos) {
	// We add the value
	if (quoted) {
		StringValueResult::AddQuotedValue(*this, buffer_pos);
	} else {
		auto value = string_t(buffer_ptr + last_position, buffer_pos - last_position);
		if (value == state_machine.options.null_str) {
			validity_mask->SetInvalid(result_position++);
		} else {
			vector_ptr[result_position++] = value;
		}
	}
	last_position = buffer_pos + 1;
}

void StringValueResult::Print() {
	for (idx_t i = 0; i < result_position; i++) {
		std::cout << vector_ptr[i].GetString();
		if (i + 1 % number_of_columns == 0) {
			std::cout << std::endl;
		} else {
			std::cout << ",";
		}
	}
}

bool StringValueResult::AddRow(StringValueResult &result, const idx_t buffer_pos) {
	// We add the value
	result.AddRowInternal(buffer_pos);

	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	if (result.result_position % result.number_of_columns != 0) {
		// If the columns are incorrect:
		// Maybe we have too many columns:
		if (result.maybe_too_many_columns) {
			auto csv_error = CSVError::IncorrectColumnAmountError(
			    result.state_machine.options, result.vector_ptr, result.result_position,
			    result.number_of_columns + result.result_position % result.number_of_columns);
			LinesPerBatch lines_per_batch(result.iterator.GetFileIdx(), result.iterator.GetBufferIdx(),
			                              result.result_position / result.number_of_columns);
			result.error_handler.Error(lines_per_batch, csv_error);
			result.result_position -= result.result_position % result.number_of_columns + result.number_of_columns;
			result.maybe_too_many_columns = false;
		}
		// Maybe we have too few columns:
		// 1) if null_padding is on we null pad it
		if (result.null_padding) {
			while (result.result_position % result.number_of_columns == 0) {
				result.validity_mask->SetInvalid(result.result_position++);
			}
		} else {
			auto csv_error = CSVError::IncorrectColumnAmountError(result.state_machine.options, result.vector_ptr,
			                                                      result.result_position,
			                                                      result.result_position % result.number_of_columns);
			LinesPerBatch lines_per_batch(result.iterator.GetFileIdx(), result.iterator.GetBufferIdx(),
			                              result.result_position / result.number_of_columns);
			result.error_handler.Error(lines_per_batch, csv_error);
			result.result_position -= result.result_position % result.number_of_columns;
		}
	}

	if (result.result_position / result.number_of_columns >= result.result_size) {
		// We have a full chunk
		return true;
	}
	return false;
}

void StringValueResult::InvalidState(StringValueResult &result) {
	// FIXME: How do we recover from an invalid state? Can we restart the state machine and jump to the next row?
	auto csv_error =
	    CSVError::UnterminatedQuotesError(result.state_machine.options, result.vector_ptr, result.result_position,
	                                      result.result_position % result.number_of_columns);
	LinesPerBatch lines_per_batch(result.iterator.GetFileIdx(), result.iterator.GetBufferIdx(),
	                              result.result_position / result.number_of_columns);
	result.error_handler.Error(lines_per_batch, csv_error);
}

bool StringValueResult::EmptyLine(StringValueResult &result, const idx_t buffer_pos) {
	// We care about empty lines if this is a single column csv file
	result.last_position = buffer_pos + 1;
	if (result.parse_chunk.ColumnCount() == 1) {
		if (result.state_machine.options.null_str.empty()) {
			result.validity_mask->SetInvalid(result.result_position++);
		}
		if (result.result_position / result.number_of_columns >= result.result_size) {
			// We have a full chunk
			return true;
		}
	}
	return false;
}

idx_t StringValueResult::NumberOfRows() {
	if (result_position % number_of_columns != 0) {
		throw InternalException("Something went wrong when reading the CSV file, more positions than columns. Open an "
		                        "issue on the issue tracker.");
	}
	return result_position / number_of_columns;
}

StringValueScanner::StringValueScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine,
                                       shared_ptr<CSVErrorHandler> error_handler, CSVIterator boundary,
                                       idx_t result_size)
    : BaseScanner(buffer_manager, state_machine, error_handler, boundary),
      result(states, *state_machine, *cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), result_size,
             iterator.pos.buffer_pos, *error_handler, iterator) {};

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	auto state_machine = make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options,
	                                                  *CSVStateMachineCache::Get(context));
	auto buffer_manager = make_shared<CSVBufferManager>(context, options, options.file_path, 0);
	return make_uniq<StringValueScanner>(buffer_manager, state_machine, make_shared<CSVErrorHandler>());
}

bool StringValueScanner::FinishedIterator() {
	return iterator.done;
}

StringValueResult *StringValueScanner::ParseChunk() {
	result.result_position = 0;
	ParseChunkInternal();
	return &result;
}

void StringValueScanner::Flush(DataChunk &insert_chunk) {
	auto process_result = ParseChunk();
	// First Get Parsed Chunk
	auto &parse_chunk = process_result->ToChunk();

	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);

	// We keep track of the borked lines, in case we are ignoring errors
	unordered_set<idx_t> borked_lines;

	// Now Do the cast-aroo
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		auto col_idx = reader_data.column_ids[c];
		auto result_idx = reader_data.column_mapping[c];
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		if (type.id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// reinterpret rather than reference, so we can deal with user-defined types
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			bool success;
			idx_t line_error = 0;

			if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::DATE).GetValue().Empty() &&
			    type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = CSVCast::TryCastDateVector(state_machine->options.dialect_options.date_format, parse_vector,
				                                     result_vector, parse_chunk.size(), error_message, line_error);
			} else if (!state_machine->options.dialect_options.date_format.at(LogicalTypeId::TIMESTAMP)
			                .GetValue()
			                .Empty() &&
			           type.id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success =
				    CSVCast::TryCastTimestampVector(state_machine->options.dialect_options.date_format, parse_vector,
				                                    result_vector, parse_chunk.size(), error_message);
			} else if (state_machine->options.decimal_separator != "." &&
			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
				success =
				    CSVCast::TryCastFloatingVectorCommaSeparated(state_machine->options, parse_vector, result_vector,
				                                                 parse_chunk.size(), error_message, type, line_error);
			} else if (state_machine->options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
				success = CSVCast::TryCastDecimalVectorCommaSeparated(
				    state_machine->options, parse_vector, result_vector, parse_chunk.size(), error_message, type);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(buffer_manager->context, parse_vector, result_vector,
				                                    parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			// An error happened, to propagate it we need to figure out the exact line where the casting failed.
			UnifiedVectorFormat inserted_column_data;
			result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
			UnifiedVectorFormat parse_column_data;
			parse_vector.ToUnifiedFormat(parse_chunk.size(), parse_column_data);
			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					break;
				}
			}
			auto csv_error =
			    CSVError::CastError(state_machine->options, parse_chunk, line_error, names[col_idx], error_message);
			LinesPerBatch lines_per_batch(iterator.GetFileIdx(), iterator.GetBufferIdx(),
			                              lines_read - parse_chunk.size() + line_error);
			error_handler->Error(lines_per_batch, csv_error);
			D_ASSERT(state_machine->options.ignore_errors);
			// We are ignoring errors. We must continue but ignoring borked rows
			for (; line_error < parse_chunk.size(); line_error++) {
				if (!inserted_column_data.validity.RowIsValid(line_error) &&
				    parse_column_data.validity.RowIsValid(line_error)) {
					borked_lines.insert(line_error);
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
	states.Initialize(CSVState::RECORD_SEPARATOR);
	if (result.result_size != 1) {
		SetStart();
	}
	result.last_position = iterator.pos.buffer_pos;
}

void StringValueScanner::Process() {
	idx_t to_pos;
	if (iterator.IsSet()) {
		to_pos = iterator.GetEndPos();
		if (to_pos > cur_buffer_handle->actual_size) {
			to_pos = cur_buffer_handle->actual_size;
		}
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	for (; iterator.pos.buffer_pos < to_pos; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result)) {
			iterator.pos.buffer_pos++;
			return;
		}
	}
}

void StringValueScanner::ProcessExtraRow() {
	idx_t to_pos = cur_buffer_handle->actual_size;
	idx_t cur_result_pos = result.result_position;
	if (result.last_position != iterator.pos.buffer_pos - 1) {
		cur_result_pos++;
	}
	if (states.IsCurrentNewRow()) {
		cur_result_pos++;
	}
	for (; iterator.pos.buffer_pos < to_pos; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result) ||
		    (result.result_position >= cur_result_pos &&
		     result.result_position % state_machine->dialect_options.num_cols == 0)) {
			return;
		}
	}
}

void StringValueScanner::ProcessOverbufferValue() {
	// Get next value
	idx_t first_buffer_pos = result.last_position;
	idx_t first_buffer_length = previous_buffer_handle->actual_size - first_buffer_pos;
	idx_t cur_value_idx = result.result_position;
	while (cur_value_idx == result.result_position) {
		ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result);
		iterator.pos.buffer_pos++;
	}
	if (iterator.pos.buffer_pos == 0) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else if (iterator.pos.buffer_pos == 1) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else {
		auto string_length = first_buffer_length + iterator.pos.buffer_pos - 2;
		auto &result_str = result.vector_ptr[result.result_position - 1];
		result_str = StringVector::EmptyString(*result.vector, string_length);
		// Copy the first buffer
		FastMemcpy(result_str.GetDataWriteable(), previous_buffer_handle->Ptr() + first_buffer_pos,
		           first_buffer_length);
		// Copy the second buffer
		FastMemcpy(result_str.GetDataWriteable() + first_buffer_length, cur_buffer_handle->Ptr(),
		           string_length - first_buffer_length);
		result_str.Finalize();
	}
}

void StringValueScanner::MoveToNextBuffer() {
	if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size) {
		previous_buffer_handle = std::move(cur_buffer_handle);
		cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
		if (!cur_buffer_handle) {
			buffer_handle_ptr = nullptr;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (states.EmptyLine() || states.NewRow()) {
				return;
			} else if (states.IsCurrentDelimiter()) {
				// we add the value
				result.AddRowInternal(previous_buffer_handle->actual_size);
				// And an extra empty value to represent what comes after the delimiter
				result.AddRowInternal(previous_buffer_handle->actual_size);
			} else {
				result.AddRowInternal(previous_buffer_handle->actual_size);
			}

			return;
		}
		iterator.pos.buffer_pos = 0;
		buffer_handle_ptr = cur_buffer_handle->Ptr();
		// Handle overbuffer value
		ProcessOverbufferValue();
		result.buffer_ptr = buffer_handle_ptr;
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
	iterator.pos.buffer_pos = row_skipper.GetIteratorPosition() + 1;
	//! FIXME: This will get borked if we skip more than one full boundary, we probably need to do the skipping before
	//! parallelizing
	if (iterator.pos.buffer_pos >= iterator.GetEndPos()) {
		throw InvalidInputException("We only support skipping lines up to the first 8 Mb of the file");
	}

	lines_read += row_skipper.GetLinesRead();
}

void StringValueScanner::SkipUntilNewLine() {
	// Now skip until next newline
	if (state_machine->options.dialect_options.state_machine_options.new_line.GetValue() ==
	    NewLineIdentifier::CARRY_ON) {
		for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
			if (buffer_handle_ptr[iterator.pos.buffer_pos] == '\n') {
				iterator.pos.buffer_pos++;
				return;
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

void StringValueScanner::SetStart() {
	if (iterator.pos.buffer_idx == 0 && iterator.pos.buffer_pos <= buffer_manager->GetStartPos()) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect, so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipCSVRows();
		return;
	}
	// We have to look for a new line that fits our schema
	while (!FinishedFile()) {
		// 1. We walk until the next new line
		SkipUntilNewLine();
		StringValueScanner scan_finder(buffer_manager, state_machine, make_shared<CSVErrorHandler>(true), iterator, 1);
		auto &tuples = *scan_finder.ParseChunk();
		if (tuples.Empty() || tuples.Size() != state_machine->options.dialect_options.num_cols) {
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			// Or if columns don't match, this is not the correct start, we need to skip until the next new line
			continue;
		}

		// 2. We try to cast all columns to the correct types
		bool all_cast = true;
		for (idx_t col_idx = 0; col_idx < tuples.Size(); col_idx++) {
			if (!tuples.GetValue(0, col_idx).TryCastAs(buffer_manager->context, types[col_idx])) {
				// We could not cast it to the right type, this is probably not the correct line start.
				all_cast = false;
				break;
			};
		}
		if (all_cast) {
			break;
		}
	}
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.result_position >= result.vector_size || iterator.done) {
		// We are done
		return;
	}
	iterator.done = true;
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (iterator.IsSet()) {
		// We read until the next line or until we have nothing else to read.
		// Move to next buffer
		MoveToNextBuffer();
		if (cur_buffer_handle) {
			ProcessExtraRow();
		}
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!FinishedFile() && result.result_position < result.vector_size) {
			MoveToNextBuffer();
			if (cur_buffer_handle) {
				Process();
			}
		}
	}
}
} // namespace duckdb
