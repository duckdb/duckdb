#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {

ScannerResult::ScannerResult(CSVStates &states_p, CSVStateMachine &state_machine_p)
    : states(states_p), state_machine(state_machine_p) {
}

idx_t ScannerResult::Size() {
	return result_position;
}

bool ScannerResult::Empty() {
	return result_position == 0;
}

BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         CSVIterator iterator_p)
    : iterator(iterator_p), buffer_manager(buffer_manager_p), state_machine(state_machine_p) {
	D_ASSERT(buffer_manager);
	D_ASSERT(state_machine);
	// Initialize current buffer handle
	cur_buffer_handle = buffer_manager->GetBuffer(iterator.GetFileIdx(), iterator.GetBufferIdx());
	buffer_handle_ptr = cur_buffer_handle->Ptr();
	D_ASSERT(cur_buffer_handle);
};

bool BaseScanner::Finished() {
	return iterator.done;
	//	if (!cur_buffer_handle) {
	//		return true;
	//	}
	//	if (buffer_manager->FileCount() > 1) {
	//		//! Fixme: We might want to lift this if we want to run the sniffer over multiple files.
	//		throw InternalException("We can't have a buffer manager that scans to infinity with more than one file");
	//	}
	//	// we have to scan to infinity, so we must check if we are done checking the whole file
	//	if (!buffer_manager->Done()) {
	//		return false;
	//	}
	//	// If yes, are we in the last buffer?
	//	if (iterator.cur_buffer_idx != buffer_manager->CachedBufferPerFile(iterator.cur_file_idx)) {
	//		return false;
	//	}
	//	// If yes, are we in the last position?
	//	return iterator.cur_buffer_pos + 1 == cur_buffer_handle->actual_size;
}

void BaseScanner::Reset() {
	iterator.SetCurrentPositionToBoundary();
}

CSVIterator &BaseScanner::GetIterator() {
	return iterator;
}

ScannerResult *BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is mot implemented");
}

ScannerResult *BaseScanner::GetResult() {
	throw InternalException("GetResult() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Process() {
	throw InternalException("Process() from CSV Base Scanner is mot implemented");
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

string BaseScanner::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names) {
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

void BaseScanner::ParseChunkInternal() {
	if (!initialized) {
		Initialize();
		initialized = true;
	}
	Process();
	FinalizeChunkProcess();
}

CSVStateMachine &BaseScanner::GetStateMachine() {
	return *state_machine;
}

} // namespace duckdb
