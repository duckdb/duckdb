#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"
#include "duckdb/execution/operator/scan/csv/parse_values.hpp"
#include "duckdb/execution/operator/scan/csv/parse_chunk.hpp"

namespace duckdb {

CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p)
    : buffer_manager(std::move(buffer_manager_p)), state_machine(std::move(state_machine_p)) {
	csv_iterator.buffer_pos = buffer_manager->GetStartPos();
};

CSVScanner::CSVScanner(ClientContext &context, CSVReaderOptions &options) {
	const vector<string> file_path {options.file_path};
	CSVStateMachineCache state_machine_cache;
	buffer_manager = make_shared<CSVBufferManager>(context, options, file_path);

	state_machine =
	    make_shared<CSVStateMachine>(options, options.dialect_options.state_machine_options, state_machine_cache);
	csv_iterator.buffer_pos = buffer_manager->GetStartPos();
}

// CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p,
//                       idx_t buffer_idx, idx_t start_buffer_p, idx_t end_buffer_p, idx_t scanner_id_p)
//    : buffer_manager(std::move(buffer_manager_p)), state_machine(std::move(state_machine_p)),
//      cur_buffer_idx(buffer_idx), start_buffer(start_buffer_p), end_buffer(end_buffer_p),
//      initial_buffer_set(buffer_idx), scanner_id(scanner_id_p) {
//	cur_pos = start_buffer;
//}

//! Skips all empty lines, until a non-empty line shows up
struct ProcessSkipEmptyLines {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.state = CSVState::STANDARD;
	}
	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char, idx_t current_pos) {
		auto state_machine = scanner.GetStateMachine();
		scanner.state = static_cast<CSVState>(
		    state_machine.transition_array[static_cast<uint8_t>(scanner.state)][static_cast<uint8_t>(current_char)]);
		if (scanner.state != CSVState::EMPTY_LINE && scanner.state != CSVState::CARRIAGE_RETURN &&
		    scanner.state != CSVState::RECORD_SEPARATOR) {
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
	Process<ProcessSkipEmptyLines>(*this, csv_iterator.buffer_pos);
}

//! Moves the buffer until the next new line
struct SkipUntilNewLine {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.state = CSVState::STANDARD;
	}
	inline static bool Process(CSVScanner &scanner, idx_t &result_pos, char current_char, idx_t current_pos) {
		auto state_machine = scanner.GetStateMachine();
		scanner.state = static_cast<CSVState>(
		    state_machine.transition_array[static_cast<uint8_t>(scanner.state)][static_cast<uint8_t>(current_char)]);
		if (scanner.state == CSVState::RECORD_SEPARATOR) {
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
	Process<SkipUntilNewLine>(*this, csv_iterator.buffer_pos);
}

bool CSVScanner::SetStart(VerificationPositions &verification_positions, const vector<LogicalType> &types) {
	if (start_set) {
		return true;
	}
	start_set = true;
	if (csv_iterator.buffer_idx == 0 && csv_iterator.buffer_pos <= buffer_manager->GetStartPos()) {
		// This means this is the very first buffer
		// This CSV is not from auto-detect, so we don't know where exactly it starts
		// Hence we potentially have to skip empty lines and headers.
		SkipEmptyLines();
		SkipHeader();
		SkipEmptyLines();
		if (verification_positions.beginning_of_first_line == 0) {
			verification_positions.beginning_of_first_line = csv_iterator.buffer_pos;
		}
		verification_positions.end_of_last_line = csv_iterator.buffer_pos;
		return true;
	}

	// We have to look for a new line that fits our schema
	bool success = false;
	while (!Finished()) {
		// 1. We walk until the next new line
		Process<SkipUntilNewLine>(*this, csv_iterator.buffer_pos);
		idx_t position_being_checked = csv_iterator.buffer_pos;
		vector<TupleOfValues> tuples(1);
		Process<ParseValues>(*this, tuples);
		if (!tuples.empty()) {
			// If no tuples were parsed, this is not the correct start, we need to skip until the next new line
			csv_iterator.buffer_pos = position_being_checked;
			continue;
		}
		vector<Value> &values = tuples[0].values;

		if (values.size() != state_machine->options.dialect_options.num_cols) {
			// If columns don't match, this is not the correct start, we need to skip until the next new line
			csv_iterator.buffer_pos = position_being_checked;
			continue;
		}
		// 2. We try to cast all columns to the correct types
		bool all_cast = true;
		for (idx_t i = 0; i < values.size(); i++) {
			if (!values[0].TryCastAs(buffer_manager->context, types[i])) {
				// We could not cast it to the right type, this is probably not the correct line start.
				all_cast = false;
				break;
			};
		}
		csv_iterator.buffer_pos = position_being_checked;
		if (all_cast) {
			// We found the start of the line, yay
			success = true;
			break;
		}
	}
	// We have to move position up to next new line
	if (verification_positions.beginning_of_first_line == 0) {
		verification_positions.beginning_of_first_line = csv_iterator.buffer_pos;
	}
	verification_positions.end_of_last_line = csv_iterator.buffer_pos;
	return success;
}

void CSVScanner::Parse(DataChunk &parse_chunk, VerificationPositions &verification_positions,
                       const vector<LogicalType> &types) {
	// If necessary we set the start of the buffer, basically where we need to start scanning from
	bool found_start = SetStart(verification_positions, types);
	if (!found_start) {
		// Nothing to Scan
		return;
	}
	// Now we do the actual parsing
	// TODO: Check for errors.
	Process<ParseChunk>(*this, parse_chunk);
	total_rows_emmited += parse_chunk.size();
}

string CSVScanner::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		auto it = sql_types_per_column.find(names[i]);
		if (it != sql_types_per_column.end()) {
			sql_types_per_column.erase(names[i]);
			continue;
		}
	}
	if (sql_types_per_column.empty()) {
		return string();
	}
	string exception = "COLUMN_TYPES error: Columns with names: ";
	for (auto &col : sql_types_per_column) {
		exception += "\"" + col.first + "\",";
	}
	exception.pop_back();
	exception += " do not exist in the CSV File";
	return exception;
}

int64_t CSVScanner::GetBufferIndex() {
	if (cur_buffer_handle) {
		return cur_buffer_handle->buffer_idx;
	}
	return -1;
}

idx_t CSVScanner::GetTotalRowsEmmited() {
	return total_rows_emmited;
}

bool CSVScanner::Finished() {
	// We consider the scanner done, if there is no buffer handle for a given buffer_idx (i.e., we are done scanning
	// the file) OR if we exhausted the bytes we were supposed to read
	return (!cur_buffer_handle && csv_iterator.buffer_idx > 0) || csv_iterator.bytes_to_read == 0;
}

void CSVIterator::Reset() {
	buffer_idx = start_buffer_idx;
	buffer_pos = start_buffer_pos;
	bytes_to_read = NumericLimits<idx_t>::Maximum();
}

void CSVScanner::Reset() {
	if (cur_buffer_handle) {
		cur_buffer_handle.reset();
	}
	csv_iterator.Reset();

	buffer_manager->Initialize();
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
