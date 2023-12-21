#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"

namespace duckdb {

StringValueResult::StringValueResult(CSVStateMachine &state_machine, CSVBufferHandle &buffer_handle)
    : number_of_columns(state_machine.dialect_options.num_cols), null_padding(state_machine.options.null_padding),
      ignore_errors(state_machine.options.ignore_errors) {
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

void StringValueResult::AddValue(StringValueResult &result, const char current_char, const idx_t buffer_pos) {
	result.vector_ptr[result.result_position++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	result.last_position = buffer_pos;
}

bool StringValueResult::AddRow(StringValueResult &result, const char current_char, const idx_t buffer_pos) {
	// We add the value
	result.vector_ptr[result.result_position++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);

	// We need to check if we are getting the correct number of columns here.
	// If columns are correct, we add it, and that's it.
	result.last_position = buffer_pos;
	if (result.result_position % result.number_of_columns != 0) {
		// If the columns are incorrect:
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

	if (result.result_position >= result.vector_size) {
		// We have a full chunk
		return true;
	}
}

void StringValueResult::Kaput(StringValueResult &result) {
	// FIXME: Add nicer error messages, also what should we do for ignore_errors?
	throw InvalidInputException("Can't parse this CSV File");
}

idx_t StringValueResult::NumberOfRows() {
	return result_position / number_of_columns;
}

StringValueScanner::StringValueScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine), result(*state_machine, *cur_buffer_handle) {};

StringValueResult *StringValueScanner::ParseChunk() {
	result.result_position = 0;
	ParseChunkInternal();
	return &result;
}

void StringValueScanner::Process() {
	idx_t to_pos;
	if (boundary.is_set) {
		to_pos = boundary.end_pos;
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
		pos.pos = 0;
		previous_buffer_handle = std::move(cur_buffer_handle);
		cur_buffer_handle = buffer_manager->GetBuffer(pos.file_id, ++pos.buffer_id);
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
	if (boundary.is_set) {
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
			Process();
		}
	}
}
} // namespace duckdb
