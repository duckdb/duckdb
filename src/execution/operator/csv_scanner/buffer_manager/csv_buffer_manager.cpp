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

void CSVBufferManager::ReadBufferEnds(shared_ptr<CSVStateMachine> state_machine) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_end_state_machine &&
	    !(buffer_end_state_machine->state_machine_options == state_machine->state_machine_options)) {
		// the ends read with another dialect are read again
		buffer_ends.clear();
	}
	buffer_end_state_machine = std::move(state_machine);
}

void CSVBufferManager::RememberBufferEnd(idx_t buffer_idx, const CSVBufferHandle &handle) {
	shared_ptr<CSVStateMachine> state_machine;
	CSVBufferEnd previous;
	bool stored = false;
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine) {
			return;
		}
		if (buffer_idx < buffer_ends.size() && buffer_ends[buffer_idx].read) {
			if (buffer_ends[buffer_idx].exact) {
				return;
			}
			stored = true;
		}
		if (buffer_idx > 0 && buffer_idx - 1 < buffer_ends.size()) {
			previous = buffer_ends[buffer_idx - 1];
		}
		state_machine = buffer_end_state_machine;
	}
	// a buffer without a line start of its own is read on from the end of the buffer before, once that one is exact
	CSVBufferEnd buffer_end;
	if (stored) {
		if (!previous.exact) {
			return;
		}
		buffer_end = ReadBufferEnd(handle.Ptr(), 0, handle.actual_size, previous.state, *state_machine);
	} else {
		buffer_end = ScanBufferEnd(handle.Ptr(), handle.actual_size, buffer_idx == 0, *state_machine);
		if (!buffer_end.exact && previous.exact) {
			buffer_end = ReadBufferEnd(handle.Ptr(), 0, handle.actual_size, previous.state, *state_machine);
		}
	}
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_idx >= buffer_ends.size()) {
		buffer_ends.resize(buffer_idx + 1);
	}
	if (!buffer_ends[buffer_idx].exact || buffer_end.exact) {
		buffer_ends[buffer_idx] = buffer_end;
	}
}

bool CSVBufferManager::GetBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end) {
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine) {
			return false;
		}
	}
	if (!LookupBufferEnd(buffer_idx, buffer_end)) {
		// the buffer was not handed out yet, its end is read once it is
		GetBuffer(buffer_idx);
		if (!LookupBufferEnd(buffer_idx, buffer_end)) {
			return false;
		}
	}
	if (!buffer_end.known && HasKnownBufferRanges()) {
		// an end that does not settle the next byte is read again behind the ends before it, by a buffer manager
		// that can hand its buffers out again
		ReadBufferEndsUpTo(buffer_idx);
		LookupBufferEnd(buffer_idx, buffer_end);
	}
	return true;
}

bool CSVBufferManager::LookupBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (buffer_idx >= buffer_ends.size() || !buffer_ends[buffer_idx].read) {
		return false;
	}
	buffer_end = buffer_ends[buffer_idx];
	return true;
}

void CSVBufferManager::ReadBufferEndsUpTo(idx_t buffer_idx) {
	// back to a buffer with an exact end, handing out those not handed out yet, then forwards again, so that every
	// end behind it is read on from the one before
	idx_t first = buffer_idx;
	while (first > 0) {
		CSVBufferEnd previous;
		bool read = LookupBufferEnd(first - 1, previous);
		if (!read) {
			GetBuffer(first - 1);
			read = LookupBufferEnd(first - 1, previous);
		}
		if (read && previous.exact) {
			break;
		}
		first--;
	}
	for (idx_t idx = first; idx <= buffer_idx; idx++) {
		GetBuffer(idx);
	}
}

CSVBufferEnd CSVBufferManager::ScanBufferEnd(const char *bytes, const idx_t size, const bool file_start,
                                             const CSVStateMachine &state_machine) {
	const auto &options = state_machine.state_machine_options;
	const char escape = options.escape.GetValue();
	if (!options.UnquotedEscapes() || escape == '\0' || escape == options.quote.GetValue()) {
		// without unquoted escapes the start search needs no more than the last byte
		CSVBufferEnd result;
		result.last_byte = size > 0 ? bytes[size - 1] : '\0';
		result.known = true;
		result.read = true;
		return result;
	}
	// the bytes are read from a line break that an even run of escapes precedes, which surely ends a line, a line
	// break behind an odd run only does inside a comment, and reading on from the sure one settles that
	const auto &transitions = state_machine.transition_array;
	const auto standard = static_cast<uint8_t>(CSVState::STANDARD);
	const bool pair_cr = transitions[static_cast<uint8_t>('\r')][standard] == CSVState::CARRIAGE_RETURN;
	auto ends_line = [&](const char byte) {
		if (byte != '\n' && byte != '\r') {
			return false;
		}
		const auto next = transitions[static_cast<uint8_t>(byte)][standard];
		return next == CSVState::RECORD_SEPARATOR || next == CSVState::CARRIAGE_RETURN;
	};
	idx_t pos = size;
	while (pos > 0) {
		pos--;
		if (!ends_line(bytes[pos])) {
			continue;
		}
		// the escapes before the line break, before the carriage return of a pair, which they escape as a whole
		if (pair_cr && bytes[pos] == '\n' && pos > 0 && bytes[pos - 1] == '\r') {
			pos--;
		}
		idx_t escapes = 0;
		while (escapes < pos && bytes[pos - 1 - escapes] == escape) {
			escapes++;
		}
		if (escapes % 2 == 0 && (escapes < pos || file_start)) {
			return ReadBufferEnd(bytes, pos, size, CSVState::STANDARD, state_machine);
		}
		// the escapes before this line break are read as part of its line
		pos -= escapes;
	}
	if (file_start) {
		return ReadBufferEnd(bytes, 0, size, CSVState::NOT_SET, state_machine);
	}
	// the line of the bytes starts before them, without an escape at their end the next byte still reads on its own
	CSVBufferEnd result;
	result.last_byte = size > 0 ? bytes[size - 1] : '\0';
	const idx_t run_end = result.last_byte == '\r' ? size - 1 : size;
	result.known = run_end == 0 || bytes[run_end - 1] != escape;
	result.read = true;
	return result;
}

CSVBufferEnd CSVBufferManager::ReadBufferEnd(const char *bytes, const idx_t from, const idx_t size,
                                             const CSVState state, const CSVStateMachine &state_machine) {
	CSVStates states;
	states.Initialize(state);
	for (idx_t i = from; i < size; i++) {
		state_machine.Transition(states, bytes[i]);
	}
	CSVBufferEnd result;
	result.last_byte = size > 0 ? bytes[size - 1] : '\0';
	result.state = states.states[1];
	result.exact = true;
	result.known = true;
	result.read = true;
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
