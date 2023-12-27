#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"

namespace duckdb {

StringValueResult::StringValueResult(CSVStates &states, CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle)
    : ScannerResult(states, state_machine), number_of_columns(state_machine.dialect_options.num_cols),
      null_padding(state_machine.options.null_padding), ignore_errors(state_machine.options.ignore_errors) {
	// Vector information
	vector_size = number_of_columns * STANDARD_VECTOR_SIZE;
	vector = make_uniq<Vector>(LogicalType::VARCHAR, vector_size);
	vector_ptr = FlatVector::GetData<string_t>(*vector);
	validity_mask = &FlatVector::Validity(*vector);

	// Buffer Information
	buffer_ptr = buffer_handle.Ptr();
	last_position = 0;

	// Current Result information
	result_position = 0;
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

void StringValueResult::ToChunk(DataChunk &parse_chunk) {
	idx_t number_of_rows = NumberOfRows();
	const auto &selection_vectors = state_machine.GetSelectionVector();
	for (idx_t col_idx = 0; col_idx < parse_chunk.ColumnCount(); col_idx++) {
		// fixme: has to do some extra checks for null padding
		auto &v = parse_chunk.data[col_idx];
		v.Slice(*vector, selection_vectors[col_idx], number_of_rows);
	}
	parse_chunk.SetCardinality(number_of_rows);
}

void StringValueResult::AddValue(StringValueResult &result, const idx_t buffer_pos) {
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
	vector_ptr[result_position++] = string_t(buffer_ptr + last_position, buffer_pos - last_position);
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

	if (result.result_position % result.number_of_columns >= result.vector_size) {
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
                                       shared_ptr<CSVStateMachine> state_machine, ScannerBoundary boundary)
    : BaseScanner(buffer_manager, state_machine, boundary), result(states, *state_machine, *cur_buffer_handle) {};

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

void StringValueScanner::Process() {
	idx_t to_pos;
	if (boundary.IsSet()) {
		to_pos = boundary.GetEndPos();
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	if (cur_buffer_handle->is_first_buffer && pos.pos == 0) {
		// This means we are in the first buffer, we must skip any empty lines, notes and the header
		// SkipSetLines();
		SkipEmptyLines();
		SkipNotes();
		//		if (state_machine->dialect_options.header.GetValue()){
		//			// If the header is set we also skip it
		//			SkipHeader();
		//		}
	}
	for (; pos.pos < to_pos; pos.pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[pos.pos], pos.pos, result)) {
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
		ProcessCharacter(*this, buffer_handle_ptr[pos.pos], pos.pos, result);
		pos.pos++;
	}
	if (pos.pos == 0) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else if (pos.pos == 1) {
		result.vector_ptr[result.result_position - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length);
	} else {
		auto string_length = first_buffer_length + pos.pos - 1;
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
	if (pos.pos == cur_buffer_handle->actual_size) {
		previous_buffer_handle = std::move(cur_buffer_handle);
		cur_buffer_handle = buffer_manager->GetBuffer(pos.file_id, ++pos.buffer_id);
		if (!cur_buffer_handle) {
			buffer_handle_ptr = nullptr;
			// This means we reached the end of the file, we must add a last line if there is any to be added
			if (result.last_position < previous_buffer_handle->actual_size) {
				result.AddRowInternal(previous_buffer_handle->actual_size);
			}
			return;
		}
		pos.pos = 0;
		buffer_handle_ptr = cur_buffer_handle->Ptr();
		// Handle overbuffer value
		ProcessOverbufferValue();
	}
}

void StringValueScanner::SkipEmptyLines() {
}

void StringValueScanner::SkipNotes() {
}

void StringValueScanner::SkipHeader() {
}

void StringValueScanner::FinalizeChunkProcess() {
	if (result.result_position >= result.vector_size) {
		// We are done
		return;
	}
	// If we are not done we have two options.
	// 1) If a boundary is set.
	if (boundary.IsSet()) {
		// We read until the next line or until we have nothing else to read.
		do {
			// Move to next buffer
			MoveToNextBuffer();
			Process();
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
