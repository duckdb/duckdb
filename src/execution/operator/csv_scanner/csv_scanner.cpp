#include <utility>

#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"

namespace duckdb {

CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p)
    : buffer_manager(std::move(buffer_manager_p)), state_machine(std::move(state_machine_p)) {
	cur_pos = buffer_manager->GetStartPos();
};

CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p, idx_t buffer_idx, idx_t start_buffer_p, idx_t end_buffer_p)
    : buffer_manager(std::move(buffer_manager_p)), state_machine(std::move(state_machine_p)), cur_buffer_idx(buffer_idx), start_buffer(start_buffer_p), end_buffer(end_buffer_p){
	cur_pos = start_buffer;
}

struct ProcessSkipEmptyLines {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.state = CSVState::STANDARD;
	}
	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char,
	                           idx_t current_pos) {
		auto state_machine = scanner.GetStateMachine();
		scanner.state = static_cast<CSVState>(
		    state_machine
		        .transition_array[static_cast<uint8_t>(scanner.state)][static_cast<uint8_t>(current_char)]);
		if (scanner.state != CSVState::EMPTY_LINE && scanner.state != CSVState::CARRIAGE_RETURN && scanner.state != CSVState::RECORD_SEPARATOR){
			result_pos = current_pos;
			return true;
		}
		// Still and empty line so we have to keep going
		return false;
	}
	inline static void Finalize(CSVScanner &scanner, idx_t &result_pos) {
		// this is a nop
		return;
	}
};

void CSVScanner::SkipEmptyLines() {
	if (state_machine->options.dialect_options.num_cols == 1) {
		// If we only have one column, empty lines are null data.
		return;
	}
	Process<ProcessSkipEmptyLines>(*this, cur_pos);
}

struct ProcessSkipHeader {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.state = CSVState::STANDARD;
	}
	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char,
	                           idx_t current_pos) {
		auto state_machine = scanner.GetStateMachine();
		scanner.state = static_cast<CSVState>(
		    state_machine
		        .transition_array[static_cast<uint8_t>(scanner.state)][static_cast<uint8_t>(current_char)]);
		if (scanner.state == CSVState::RECORD_SEPARATOR){
			// Next Position is the first line.
			result_pos = current_pos + 1;
			return true;
		}
		// Still reading the header so we have to keep going
		return false;
	}
	inline static void Finalize(CSVScanner &scanner, idx_t &result_pos) {
		// this is a nop
		return;
	}
};

void CSVScanner::SkipHeader() {
	if (!state_machine->options.has_header || !state_machine->options.dialect_options.header) {
		// No header to skip
		return;
	}
	Process<ProcessSkipHeader>(*this, cur_pos);
}

void CSVScanner::InitializeBufferHandle();

bool CSVScanner::SetStart(VerificationPositions& verification_positions){
	if (cur_buffer_idx == 0 && start_buffer <= buffer_manager->GetStartPos()) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipEmptyLines();
		SkipHeader();
		SkipEmptyLines();
		if (verification_positions.beginning_of_first_line == 0) {
			verification_positions.beginning_of_first_line = cur_pos;
		}
		verification_positions.end_of_last_line = cur_pos;
		return true;
	}
	// If this is not the first buffer we must move buffer to next new line

	// We try to cast the new line

	// If we succeeded, that's our start

	// We have to move position up to next new line
	idx_t end_buffer_real = end_buffer;
	// Check if we already start in a valid line
	string error_message;
	bool successfully_read_first_line = false;
	while (!successfully_read_first_line) {
		DataChunk first_line_chunk;
		first_line_chunk.Initialize(allocator, return_types);
		// Ensure that parse_chunk has no gunk when trying to figure new line
		parse_chunk.Reset();
		for (; position_buffer < end_buffer; position_buffer++) {
			if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
				bool carriage_return = (*buffer)[position_buffer] == '\r';
				bool carriage_return_followed = false;
				position_buffer++;
				if (position_buffer < end_buffer) {
					if (carriage_return && (*buffer)[position_buffer] == '\n') {
						carriage_return_followed = true;
						position_buffer++;
					}
				}
				if (NewLineDelimiter(carriage_return, carriage_return_followed, position_buffer - 1 == start_buffer)) {
					break;
				}
			}
		}
		SkipEmptyLines();

		if (position_buffer > buffer_size) {
			break;
		}

		if (position_buffer >= end_buffer && !StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1])) {
			break;
		}

		if (position_buffer > end_buffer && options.dialect_options.new_line == NewLineIdentifier::CARRY_ON &&
		    (*buffer)[position_buffer - 1] == '\n') {
			break;
		}
		idx_t position_set = position_buffer;
		start_buffer = position_buffer;
		// We check if we can add this line
		// disable the projection pushdown while reading the first line
		// otherwise the first line parsing can be influenced by which columns we are reading
		auto column_ids = std::move(reader_data.column_ids);
		auto column_mapping = std::move(reader_data.column_mapping);
		InitializeProjection();
		try {
			successfully_read_first_line = Parse(first_line_chunk, error_message, true);
		} catch (...) {
			successfully_read_first_line = false;
		}
		// restore the projection pushdown
		reader_data.column_ids = std::move(column_ids);
		reader_data.column_mapping = std::move(column_mapping);
		end_buffer = end_buffer_real;
		start_buffer = position_set;
		if (position_buffer >= end_buffer) {
			if (successfully_read_first_line) {
				position_buffer = position_set;
			}
			break;
		}
		position_buffer = position_set;
	}
	if (verification_positions.beginning_of_first_line == 0) {
		verification_positions.beginning_of_first_line = position_buffer;
	}
	// Ensure that parse_chunk has no gunk when trying to figure new line
	parse_chunk.Reset();

	verification_positions.end_of_last_line = position_buffer;
	finished = false;
	return successfully_read_first_line;
}

bool CSVScanner::Finished() {
	return !cur_buffer_handle;
}

void CSVScanner::Reset() {
	if (cur_buffer_handle) {
		cur_buffer_handle.reset();
	}
	if (cur_buffer_idx > 0) {
		buffer_manager->UnpinBuffer(cur_buffer_idx - 1);
	}
	cur_buffer_idx = 0;
	buffer_manager->Initialize();
	cur_pos = buffer_manager->GetStartPos();
}

CSVStateMachineSniffing &CSVScanner::GetStateMachineSniff() {
	D_ASSERT(state_machine);
	CSVStateMachineSniffing *sniffing_state_machine = static_cast<CSVStateMachineSniffing *>(state_machine.get());
	return *sniffing_state_machine;
}

CSVStateMachine &CSVScanner::GetStateMachine() {
	D_ASSERT(state_machine);
	return *state_machine;
}

void CSVScanner::VerifyUTF8() {
	auto utf_type = Utf8Proc::Analyze(value.c_str(), value.size());
	if (utf_type == UnicodeType::INVALID) {
		int64_t error_line = cur_rows;
		throw InvalidInputException("Error in file \"%s\" at line %llu: "
		                            "%s. Parser options:\n%s",
		                            state_machine->options.file_path, error_line,
		                            ErrorManager::InvalidUnicodeError(value, "CSV file"),
		                            state_machine->options.ToString());
	}
}
} // namespace duckdb
