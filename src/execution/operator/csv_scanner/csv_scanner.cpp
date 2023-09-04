#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"

namespace duckdb {

CSVScanner::CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p)
    : buffer_manager(std::move(buffer_manager_p)), state_machine(std::move(state_machine_p)) {
	cur_pos = buffer_manager->GetStartPos();
};

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

CSVStateMachineSniffing &CSVScanner::GetStateMachine() {
	D_ASSERT(state_machine);
	CSVStateMachineSniffing *sniffing_state_machine = static_cast<CSVStateMachineSniffing *>(state_machine.get());
	return *sniffing_state_machine;
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
