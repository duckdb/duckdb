#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"

namespace duckdb {

void StringValueResult::AddValue(StringValueResult &result, const char current_char, const idx_t buffer_pos) {
	result.vector_ptr[result.result_position++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	result.last_position = buffer_pos;
}

bool StringValueResult::AddRow(StringValueResult &result, const char current_char, const idx_t buffer_pos) {
	result.vector_ptr[result.result_position++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	result.last_position = buffer_pos;

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
	return result_position / column_count;
}

StringValueScanner::StringValueScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine) {
	// Set up the result
	result.vector_size = state_machine->dialect_options.num_cols * STANDARD_VECTOR_SIZE;
	result.vector = make_uniq<Vector>(LogicalType::VARCHAR, result.vector_size);
	result.vector_ptr = FlatVector::GetData<string_t>(*result.vector);
	result.last_position = cur_buffer_handle->start_position;
	result.result_position = 0;
	result.buffer_ptr = cur_buffer_handle->Ptr();
};

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
