#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_sequential_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_random_access_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/function/table/read_csv.hpp"
namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, const CSVReaderOptions &options,
                                   const OpenFileInfo &file_p, bool per_file_single_threaded_p,
                                   unique_ptr<CSVFileHandle> file_handle_p)
    : file_handle(std::move(file_handle_p)), context(context_p), per_file_single_threaded(per_file_single_threaded_p),
      file(file_p), buffer_size(options.buffer_size_option.GetValue()) {
	D_ASSERT(file_handle);
}

shared_ptr<CSVBufferManager> CSVBufferManager::Open(ClientContext &context, const CSVReaderOptions &options,
                                                    const OpenFileInfo &file, bool per_file_single_threaded,
                                                    unique_ptr<CSVFileHandle> file_handle) {
	D_ASSERT(!file.path.empty());
	if (!file_handle) {
		file_handle = ReadCSV::OpenCSV(file, options, context);
	}
	if (file_handle->HasKnownBufferRanges()) {
		return make_shared_ptr<CSVRandomAccessBufferManager>(context, options, file, per_file_single_threaded,
		                                                     std::move(file_handle));
	}
	return make_shared_ptr<CSVSequentialBufferManager>(context, options, file, per_file_single_threaded,
	                                                   std::move(file_handle));
}

bool CSVBufferManager::HasKnownBufferRanges() const {
	return file_handle->HasKnownBufferRanges();
}

idx_t CSVBufferManager::KnownBufferCount() const {
	D_ASSERT(HasKnownBufferRanges());
	const idx_t file_size = file_handle->FileSize();
	if (file_size == 0) {
		// an empty file still materializes a single empty buffer
		return 1;
	}
	return (file_size + buffer_size - 1) / buffer_size;
}

idx_t CSVBufferManager::KnownBufferSize(const idx_t buffer_idx) const {
	D_ASSERT(HasKnownBufferRanges() && buffer_idx < KnownBufferCount());
	const idx_t file_size = file_handle->FileSize();
	const idx_t buffer_start = buffer_idx * buffer_size;
	return MinValue<idx_t>(buffer_size, file_size - buffer_start);
}

void CSVBufferManager::ReadBufferEnds(shared_ptr<CSVStateMachine> state_machine, idx_t max_line_size) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_end_state_machine &&
	    !(buffer_end_state_machine->state_machine_options == state_machine->state_machine_options)) {
		// the ends read with another dialect are read again
		buffer_ends.clear();
	}
	buffer_end_state_machine = std::move(state_machine);
	buffer_end_max_line_size = max_line_size;
}

void CSVBufferManager::RememberBufferEnd(idx_t buffer_idx, const CSVBufferHandle &handle) {
	shared_ptr<CSVStateMachine> state_machine;
	idx_t max_line_size;
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine ||
		    (buffer_idx < buffer_ends.size() && buffer_ends[buffer_idx].escape != CSVBufferEndEscape::NOT_READ)) {
			return;
		}
		state_machine = buffer_end_state_machine;
		max_line_size = buffer_end_max_line_size;
	}
	const auto buffer_end =
	    ScanBufferEnd(handle.Ptr(), handle.actual_size, buffer_idx == 0, *state_machine, max_line_size);
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_idx >= buffer_ends.size()) {
		buffer_ends.resize(buffer_idx + 1);
	}
	buffer_ends[buffer_idx] = buffer_end;
}

bool CSVBufferManager::GetBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end) {
	for (idx_t attempt = 0; attempt < 2; attempt++) {
		{
			lock_guard<mutex> parallel_lock(main_mutex);
			if (!buffer_end_state_machine) {
				return false;
			}
			if (buffer_idx < buffer_ends.size() && buffer_ends[buffer_idx].escape != CSVBufferEndEscape::NOT_READ) {
				buffer_end = buffer_ends[buffer_idx];
				return true;
			}
		}
		// the buffer was not handed out yet, its end is read once it is
		GetBuffer(buffer_idx);
	}
	return false;
}

CSVBufferEnd CSVBufferManager::ScanBufferEnd(const char *bytes, const idx_t size, const bool file_start,
                                             const CSVStateMachine &state_machine, const idx_t max_line_size) {
	CSVBufferEnd result;
	result.escape = CSVBufferEndEscape::NONE;
	if (size == 0) {
		return result;
	}
	result.last_byte = bytes[size - 1];
	const auto &options = state_machine.state_machine_options;
	const char escape = options.escape.GetValue();
	if (!options.UnquotedEscapes() || escape == '\0' || escape == options.quote.GetValue()) {
		return result;
	}
	// the escapes that matter end the bytes or precede their carriage return
	const idx_t run_end = result.last_byte == '\r' ? size - 1 : size;
	if (run_end == 0 || bytes[run_end - 1] != escape) {
		return result;
	}
	// the line of those escapes is read from a line break that an even run of escapes precedes, which surely ends a
	// line, a line break behind an odd run only does inside a comment, and reading on from the sure one settles that
	const auto &transitions = state_machine.transition_array;
	const auto standard = static_cast<uint8_t>(CSVState::STANDARD);
	auto ends_line = [&](const char byte) {
		if (byte != '\n' && byte != '\r') {
			return false;
		}
		const auto next = transitions[static_cast<uint8_t>(byte)][standard];
		return next == CSVState::RECORD_SEPARATOR || next == CSVState::CARRIAGE_RETURN;
	};
	idx_t pos = run_end;
	idx_t line_bytes = 0;
	bool line_break_found = false;
	while (pos > 0) {
		pos--;
		if (!ends_line(bytes[pos])) {
			if (++line_bytes > max_line_size) {
				result.escape = CSVBufferEndEscape::UNKNOWN;
				return result;
			}
			continue;
		}
		line_bytes = 0;
		idx_t escapes = 0;
		while (escapes < pos && bytes[pos - 1 - escapes] == escape) {
			escapes++;
		}
		if (escapes % 2 == 0 && (escapes < pos || file_start)) {
			line_break_found = true;
			break;
		}
		// the escapes before this line break are read as part of its line
		pos -= escapes;
	}
	if (!line_break_found && !file_start) {
		result.escape = CSVBufferEndEscape::UNKNOWN;
		return result;
	}
	CSVStates states;
	states.Initialize(line_break_found ? CSVState::STANDARD : CSVState::NOT_SET);
	for (idx_t i = pos; i < size; i++) {
		state_machine.Transition(states, bytes[i]);
	}
	if (states.IsState(CSVState::UNQUOTED_ESCAPE)) {
		result.escape = CSVBufferEndEscape::ESCAPED;
	} else if (states.IsState(CSVState::ESCAPED_RETURN)) {
		result.escape = CSVBufferEndEscape::ESCAPED_RETURN;
	}
	return result;
}

idx_t CSVBufferManager::GetBufferSize() const {
	return buffer_size;
}

idx_t CSVBufferManager::BufferCount() const {
	lock_guard<mutex> parallel_lock(main_mutex);
	return cached_buffers.size();
}

string CSVBufferManager::GetFilePath() const {
	return file.path;
}

bool CSVBufferManager::IsBlockUnloaded(idx_t block_idx) {
	if (block_idx < cached_buffers.size()) {
		return cached_buffers[block_idx]->IsUnloaded();
	}
	return false;
}

} // namespace duckdb
