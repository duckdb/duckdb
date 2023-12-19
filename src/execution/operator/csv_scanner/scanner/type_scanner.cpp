#include "duckdb/execution/operator/scan/csv/scanner/string_value_scanner.hpp"

namespace duckdb {

void TypeResult::AddValue(TypeResult &result, const char current_char, const idx_t buffer_pos) {
	result.vector_ptr[result.cur_value_idx++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	result.last_position = buffer_pos;
}

bool TypeResult::AddRow(TypeResult &result, const char current_char, const idx_t buffer_pos) {
	result.vector_ptr[result.cur_value_idx++] =
	    string_t(result.buffer_ptr + result.last_position, buffer_pos - result.last_position);
	result.last_position = buffer_pos;

	if (result.cur_value_idx >= result.vector_size) {
		// We have a full chunk
		return true;
	}
}

void TypeResult::Kaput(TypeResult &result) {
	// FIXME: Add nicer error message
	throw InvalidInputException("Can't parse this CSV File");
}

TypeScanner::TypeScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine) {
	result.vector_size = state_machine->dialect_options.num_cols * STANDARD_VECTOR_SIZE;
	result.vector = make_uniq<Vector>(LogicalType::VARCHAR, result.vector_size);
	result.vector_ptr = FlatVector::GetData<string_t>(*result.vector);
	result.last_position = cur_buffer_handle->start_position;
	result.cur_value_idx = 0;
	result.buffer_ptr = cur_buffer_handle->Ptr();
};

TypeResult *TypeScanner::ParseChunk() {
	result.cur_value_idx = 0;
	ParseChunkInternal();
	return &result;
}

void TypeScanner::Process() {
	// Run on this buffer
	for (; pos.pos < cur_buffer_handle->actual_size; pos.pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[pos.pos], pos.pos, result)) {
			return;
		}
	}
}

void TypeScanner::ProcessOverbufferValue() {
	// Get next value
	idx_t first_buffer_pos = result.last_position;
	idx_t first_buffer_length = previous_buffer_handle->actual_size - first_buffer_pos;
	idx_t cur_value_idx = result.cur_value_idx;
	while (cur_value_idx == result.cur_value_idx) {
		ProcessCharacter(*this, buffer_handle_ptr[pos.pos], pos.pos, result);
		pos.pos++;
	}
	if (pos.pos == 0) {
		result.vector_ptr[result.cur_value_idx - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length - 1);
	} else if (pos.pos == 1) {
		result.vector_ptr[result.cur_value_idx - 1] =
		    string_t(previous_buffer_handle->Ptr() + first_buffer_pos, first_buffer_length);
	} else {
		auto string_length = first_buffer_length + pos.pos - 1;
		auto &result_str = result.vector_ptr[result.cur_value_idx - 1];
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

void TypeScanner::FinalizeChunkProcess() {
	if (result.cur_value_idx >= result.vector_size) {
		// We are done
		return;
	}
	// We run until we have a full chunk, or we are done scanning
	while (!Finished() && result.cur_value_idx < result.vector_size) {
		if (pos.pos == cur_buffer_handle->actual_size) {
			// Move to next buffer
			pos.pos = 0;
			previous_buffer_handle = std::move(cur_buffer_handle);
			cur_buffer_handle = buffer_manager->GetBuffer(pos.file_id, ++pos.buffer_id);
			buffer_handle_ptr = cur_buffer_handle->Ptr();
			// Handle overbuffer value
		}
		Process();
	}
}
} // namespace duckdb
