#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"

#include "duckdb/execution/operator/scan/csv/csv_casting.hpp"

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle,
                                     Allocator &buffer_allocator, idx_t result_size_p, idx_t buffer_position)
    : ScannerResult(states, state_machine), number_of_columns(state_machine.dialect_options.num_cols),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors),
      result_size(result_size_p) {
	// Vector information
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

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
	D_ASSERT(result.result_position < result.vector_size);
	result.vector_ptr[result.result_position++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position - 1);
	result.last_position = buffer_pos;
	if (result.result_position % result.number_of_columns == 0) {
		// This means this value reached the number of columns in a line. This is fine, if this is the last buffer, and
		// the last buffer position However, if that's not the case, this means we might be reading too many columns.
		result.maybe_too_many_columns = true;
	}
}

inline void StringValueResult::AddRowInternal(idx_t buffer_pos) {
	// We add the value
	vector_ptr[result_position++] = string_t(buffer_ptr + last_position, buffer_pos - last_position - 1);
	last_position = buffer_pos;
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
			if (result.ignore_errors) {
				// 1) if ignore_errors is on, we invalidate the whole line.
				result.result_position -= result.result_position % result.number_of_columns + result.number_of_columns;
			} else {
				// 2) otherwise we error.
				throw InvalidInputException("I'm a baddie");
			}
			result.maybe_too_many_columns = false;
		}
		// Maybe we have too few columns:
		// 1) if null_padding is on we null pad it
		if (result.null_padding) {
			while (result.result_position % result.number_of_columns == 0) {
				result.validity_mask->SetInvalid(result.result_position++);
			}
		} else if (result.ignore_errors) {
			// 2) if ignore_errors is on, we invalidate the whole line.
			result.result_position -= result.result_position % result.number_of_columns;
		} else {
			// 3) otherwise we error.
			throw InvalidInputException("I'm a baddie");
		}
	}

	if (result.result_position / result.number_of_columns >= result.result_size) {
		// We have a full chunk
		return true;
	}
	return false;
}

void StringValueResult::Kaput(StringValueResult &result) {
	// FIXME: Add nicer error messages, also what should we do for ignore_errors?
	throw InvalidInputException("Can't parse this CSV File");
}

idx_t StringValueResult::NumberOfRows() {
	D_ASSERT(result_position % number_of_columns == 0);
	return result_position / number_of_columns;
}

StringValueScanner::StringValueScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine, CSVIterator boundary,
                                       idx_t result_size)
    : BaseScanner(buffer_manager, state_machine, boundary),
      result(states, *state_machine, *cur_buffer_handle, BufferAllocator::Get(buffer_manager->context), result_size,
             iterator.cur_buffer_pos) {};

unique_ptr<StringValueScanner> StringValueScanner::GetCSVScanner(ClientContext &context, CSVReaderOptions &options) {
	CSVStateMachineCache cache;
	auto state_machine = make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options, cache);
	vector<string> file_paths = {options.file_path};
	auto buffer_manager = make_shared<CSVBufferManager>(context, options, file_paths);
	return make_uniq<StringValueScanner>(buffer_manager, state_machine);
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

	//	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);

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
			//			boundary.Print();
			//			pos.Print();
			throw InvalidInputException("error");
		}
	}
}

void StringValueScanner::Process() {
	SetStart();
	idx_t to_pos;
	if (iterator.IsSet()) {
		to_pos = iterator.GetEndPos();
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	for (; iterator.cur_buffer_pos < to_pos; iterator.cur_buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.cur_buffer_pos], iterator.cur_buffer_pos, result)) {
			return;
		}
	}
}

void StringValueScanner::ProcessExtraRow() {
	idx_t to_pos = cur_buffer_handle->actual_size;
	idx_t cur_result_pos = result.result_position + 1;
	for (; iterator.cur_buffer_pos < to_pos; iterator.cur_buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.cur_buffer_pos], iterator.cur_buffer_pos, result) ||
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
		ProcessCharacter(*this, buffer_handle_ptr[iterator.cur_buffer_pos], iterator.cur_buffer_pos, result);
		iterator.cur_buffer_pos++;
	}
	if (iterator.cur_buffer_pos == 0) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else if (iterator.cur_buffer_pos == 1) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else {
		auto string_length = first_buffer_length + iterator.cur_buffer_pos - 2;
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
	if (iterator.cur_buffer_pos == cur_buffer_handle->actual_size) {
		previous_buffer_handle = std::move(cur_buffer_handle);
		cur_buffer_handle = buffer_manager->GetBuffer(iterator.cur_file_idx, ++iterator.cur_buffer_idx);
		if (!cur_buffer_handle) {
			buffer_handle_ptr = nullptr;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (result.last_position < previous_buffer_handle->actual_size) {
				result.AddRowInternal(previous_buffer_handle->actual_size);
			}
			return;
		}
		iterator.cur_buffer_pos = 0;
		buffer_handle_ptr = cur_buffer_handle->Ptr();
		// Handle overbuffer value
		ProcessOverbufferValue();
		result.buffer_ptr = buffer_handle_ptr;
	}
}

void StringValueScanner::SkipEmptyLines() {
}

void StringValueScanner::SkipNotes() {
}

void StringValueScanner::SkipHeader() {
	SkipUntilNewLine();
	result.last_position = iterator.cur_buffer_pos;
}

void StringValueScanner::SkipUntilNewLine() {
	if (state_machine->options.dialect_options.new_line.GetValue() == NewLineIdentifier::CARRY_ON) {
		for (; iterator.cur_buffer_pos < cur_buffer_handle->actual_size; iterator.cur_buffer_pos++) {
			if (buffer_handle_ptr[iterator.cur_buffer_pos] == '\n') {
				iterator.cur_buffer_pos++;
				return;
			}
		}
	} else {
		for (; iterator.cur_buffer_pos < cur_buffer_handle->actual_size; iterator.cur_buffer_pos++) {
			if (buffer_handle_ptr[iterator.cur_buffer_pos] == '\n' ||
			    buffer_handle_ptr[iterator.cur_buffer_pos] == '\r') {
				iterator.cur_buffer_pos++;
				return;
			}
		}
	}
}

bool StringValueScanner::SetStart() {
	if (start_set) {
		return true;
	}
	start_set = true;
	if (iterator.cur_buffer_idx == 0 && iterator.cur_buffer_pos <= buffer_manager->GetStartPos()) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect, so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipEmptyLines();
		SkipHeader();
		SkipEmptyLines();
		return true;
	}

	// We have to look for a new line that fits our schema
	bool success = false;
	while (!Finished()) {
		// 1. We walk until the next new line
		SkipUntilNewLine();
		idx_t position_being_checked = iterator.cur_buffer_pos;
		StringValueScanner scan_finder(buffer_manager, state_machine, iterator, 1);
		scan_finder.start_set = true;
		auto &tuples = *scan_finder.ParseChunk();
		if (tuples.Empty()) {
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			iterator.cur_buffer_pos = position_being_checked;
			continue;
		}
		if (tuples.Size() != state_machine->options.dialect_options.num_cols) {
			// If columns don't match, this is not the correct start, we need to skip until the next new line
			iterator.cur_buffer_pos = position_being_checked;
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
		iterator.cur_buffer_pos = position_being_checked;
		if (all_cast) {
			// We found the start of the line, yay
			success = true;
			break;
		}
	}
	result.last_position = iterator.cur_buffer_pos;
	return success;
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
		do {
			// Move to next buffer
			MoveToNextBuffer();
			ProcessExtraRow();
		} while (!Finished() && result.result_position % state_machine->dialect_options.num_cols != 0);
	} else {
		// 2) If a boundary is not set
		// We read until the chunk is complete, or we have nothing else to read.
		while (!Finished() && result.result_position < result.vector_size) {
			MoveToNextBuffer();
			if (cur_buffer_handle) {
				Process();
			}
		}
	}
}
} // namespace duckdb
