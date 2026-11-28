#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_sequential_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_random_access_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_byte_skipper.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/common/string_util.hpp"
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

void CSVBufferManager::ReadBufferEnds(shared_ptr<CSVStateMachine> state_machine, const idx_t range_size_p,
                                      const idx_t scan_start_buffer_p, const idx_t scan_start_pos_p) {
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (buffer_end_state_machine &&
		    (!(buffer_end_state_machine->state_machine_options == state_machine->state_machine_options) ||
		     range_size != range_size_p || scan_start_buffer != scan_start_buffer_p ||
		     scan_start_pos != scan_start_pos_p)) {
			// the ends read with another dialect, range grid or scan start are read again
			buffer_ends.clear();
			range_states.clear();
		}
		exact_ends = ReadsExactEnds(state_machine->state_machine_options);
		buffer_end_state_machine = std::move(state_machine);
		range_size = range_size_p;
		scan_start_buffer = scan_start_buffer_p;
		scan_start_pos = scan_start_pos_p;
	}
	// the end of the buffer the scan starts in anchors the ends behind it
	GetBuffer(scan_start_buffer_p);
}

idx_t CSVBufferManager::ScanStartIn(idx_t buffer_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	return buffer_idx == scan_start_buffer ? scan_start_pos : DConstants::INVALID_INDEX;
}

bool CSVBufferManager::ReadsExactEnds(const CSVStateMachineOptions &options) {
	const char escape = options.escape.GetValue();
	return options.UnquotedEscapes() && escape != '\0' && escape != options.quote.GetValue();
}

idx_t CSVBufferManager::RangeStatesPerBuffer() const {
	// the range starts inside a buffer, the first range of a buffer starts at the buffer start
	return range_size == 0 ? 0 : (buffer_size + range_size - 1) / range_size - 1;
}

void CSVBufferManager::RememberBufferEnd(idx_t buffer_idx, const CSVBufferHandle &handle) {
	shared_ptr<CSVStateMachine> state_machine;
	CSVBufferEnd previous;
	bool stored;
	idx_t range_size_p;
	idx_t range_count;
	idx_t scan_start;
	{
		unique_lock<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine) {
			return;
		}
		while (buffer_idx > 0 && buffer_idx - 1 < buffer_ends.size() && buffer_ends[buffer_idx - 1].reading) {
			// the end before this one is about to be published, this one may read on from it
			buffer_end_read.wait(parallel_lock);
		}
		if (buffer_idx >= buffer_ends.size()) {
			buffer_ends.resize(buffer_idx + 1);
		}
		auto &end = buffer_ends[buffer_idx];
		if (end.reading || (end.read && end.exact)) {
			// read already, or another thread reads it right now
			return;
		}
		if (buffer_idx > 0) {
			previous = buffer_ends[buffer_idx - 1];
		}
		stored = end.read;
		if (stored && !previous.exact) {
			// an end without a line start of its own waits for an exact end before it
			return;
		}
		end.reading = true;
		state_machine = buffer_end_state_machine;
		range_size_p = range_size;
		range_count = RangeStatesPerBuffer();
		scan_start = buffer_idx == scan_start_buffer ? scan_start_pos : DConstants::INVALID_INDEX;
	}
	// a buffer without a line start of its own is read on from the end of the buffer before, once that one is exact
	try {
		vector<CSVState> states(range_count, CSVState::NOT_SET);
		CSVBufferEnd buffer_end;
		if (!stored) {
			buffer_end =
			    ScanBufferEnd(handle.Ptr(), handle.actual_size, scan_start, *state_machine, range_size_p, states);
		}
		if (!buffer_end.exact && previous.exact) {
			buffer_end = ReadBufferEnd(handle.Ptr(), 0, handle.actual_size, previous.state, *state_machine,
			                           range_size_p, states);
		}
		lock_guard<mutex> parallel_lock(main_mutex);
		if (buffer_end.exact && range_count > 0 && range_count == RangeStatesPerBuffer()) {
			if (range_states.size() < (buffer_idx + 1) * range_count) {
				range_states.resize((buffer_idx + 1) * range_count, CSVState::NOT_SET);
			}
			std::copy(states.begin(), states.end(),
			          range_states.begin() + NumericCast<int64_t>(buffer_idx * range_count));
		}
		if (buffer_idx >= buffer_ends.size()) {
			buffer_ends.resize(buffer_idx + 1);
		}
		auto &end = buffer_ends[buffer_idx];
		end.reading = false;
		if (!end.exact || buffer_end.exact) {
			end = buffer_end;
		}
	} catch (...) {
		{
			lock_guard<mutex> parallel_lock(main_mutex);
			if (buffer_idx < buffer_ends.size()) {
				buffer_ends[buffer_idx].reading = false;
			}
		}
		buffer_end_read.notify_all();
		throw;
	}
	buffer_end_read.notify_all();
}

bool CSVBufferManager::GetBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end, const bool through_inexact) {
	bool exact_ends_p;
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine) {
			return false;
		}
		exact_ends_p = exact_ends;
	}
	if (!LookupBufferEnd(buffer_idx, buffer_end)) {
		// the buffer was not handed out yet, its end is read once it is
		GetBuffer(buffer_idx);
		if (!LookupBufferEnd(buffer_idx, buffer_end)) {
			return false;
		}
	}
	if (!buffer_end.exact && exact_ends_p && HasKnownBufferRanges()) {
		// read on from the exact end before it, through inexact ends when asked to or when the end settles nothing
		ReadBufferEndsUpTo(buffer_idx, through_inexact || !buffer_end.known);
		LookupBufferEnd(buffer_idx, buffer_end);
	}
	return true;
}

bool CSVBufferManager::GetRangeStart(idx_t buffer_idx, idx_t buffer_pos, CSVBufferEnd &range_start,
                                     const bool through_inexact) {
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		if (!buffer_end_state_machine || !exact_ends || range_size == 0 || buffer_pos == 0 ||
		    buffer_pos % range_size != 0) {
			return false;
		}
	}
	// the state at the range start was kept when the end of its buffer was read exactly
	CSVBufferEnd buffer_end;
	if (!GetBufferEnd(buffer_idx, buffer_end, through_inexact) || !buffer_end.exact) {
		return false;
	}
	lock_guard<mutex> parallel_lock(main_mutex);
	const idx_t range_count = RangeStatesPerBuffer();
	const idx_t range_idx = buffer_pos / range_size;
	if (range_idx == 0 || range_idx > range_count) {
		return false;
	}
	const idx_t state_idx = buffer_idx * range_count + range_idx - 1;
	if (state_idx >= range_states.size() || range_states[state_idx] == CSVState::NOT_SET) {
		return false;
	}
	range_start.state = range_states[state_idx];
	range_start.exact = true;
	range_start.known = true;
	range_start.read = true;
	return true;
}

bool CSVBufferManager::LookupBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end) {
	unique_lock<mutex> parallel_lock(main_mutex);
	while (buffer_idx < buffer_ends.size() && buffer_ends[buffer_idx].reading) {
		buffer_end_read.wait(parallel_lock);
	}
	if (buffer_idx >= buffer_ends.size() || !buffer_ends[buffer_idx].read) {
		return false;
	}
	buffer_end = buffer_ends[buffer_idx];
	return true;
}

void CSVBufferManager::ReadBufferEndsUpTo(idx_t buffer_idx, bool through_inexact) {
	// back to a buffer with an exact end, handing out those not handed out yet, then forwards again, so that every
	// end behind it is read on from the one before
	idx_t first = buffer_idx;
	idx_t floor;
	{
		lock_guard<mutex> parallel_lock(main_mutex);
		floor = scan_start_buffer;
	}
	while (first > floor) {
		CSVBufferEnd previous;
		bool read = LookupBufferEnd(first - 1, previous);
		if (!read) {
			GetBuffer(first - 1);
			read = LookupBufferEnd(first - 1, previous);
		}
		if (read && previous.exact) {
			break;
		}
		if (!through_inexact) {
			// nothing to read this end on from
			return;
		}
		first--;
	}
	for (idx_t idx = first; idx <= buffer_idx; idx++) {
		GetBuffer(idx);
		CSVBufferEnd end;
		if (LookupBufferEnd(idx, end) && !end.exact) {
			// read before the end before it was exact, read on from it now
			GetBuffer(idx);
			LookupBufferEnd(idx, end);
		}
		if (!end.exact) {
			return;
		}
	}
}

CSVBufferEnd CSVBufferManager::ScanBufferEnd(const char *bytes, const idx_t size, const idx_t scan_start,
                                             const CSVStateMachine &state_machine) {
	vector<CSVState> range_states;
	return ScanBufferEnd(bytes, size, scan_start, state_machine, 0, range_states);
}

CSVBufferEnd CSVBufferManager::ScanBufferEnd(const char *bytes, const idx_t size, const idx_t scan_start,
                                             const CSVStateMachine &state_machine, const idx_t range_size,
                                             vector<CSVState> &range_states) {
	const auto &options = state_machine.state_machine_options;
	const char escape = options.escape.GetValue();
	const char quote = options.quote.GetValue();
	if (!ReadsExactEnds(options)) {
		// without unquoted escapes the start search needs no more than the last byte
		CSVBufferEnd result;
		result.last_byte = size > 0 ? bytes[size - 1] : '\0';
		result.known = true;
		result.read = true;
		return result;
	}
	const auto &transitions = state_machine.transition_array;
	const auto standard = static_cast<uint8_t>(CSVState::STANDARD);
	const bool scan_starts = scan_start != DConstants::INVALID_INDEX;
	if (quote == '\0') {
		// without quotes a line break behind an even run of escapes surely ends a line, one behind an odd run only
		// inside a comment, with quotes any line break may lie inside a quoted value
		const bool pair_cr = transitions[static_cast<uint8_t>('\r')][standard] == CSVState::CARRIAGE_RETURN;
		auto ends_line = [&](const char byte) {
			if (byte != '\n' && byte != '\r') {
				return false;
			}
			const auto next = transitions[static_cast<uint8_t>(byte)][standard];
			return next == CSVState::RECORD_SEPARATOR || next == CSVState::CARRIAGE_RETURN;
		};
		const idx_t floor = scan_starts ? MinValue<idx_t>(scan_start, size) : 0;
		idx_t pos = size;
		while (pos > floor) {
			pos--;
			if (!ends_line(bytes[pos])) {
				continue;
			}
			// the escapes before the line break, before the carriage return of a pair, which they escape as a whole
			if (pair_cr && bytes[pos] == '\n' && pos > 0 && bytes[pos - 1] == '\r') {
				pos--;
			}
			idx_t escapes = 0;
			while (escapes < pos - floor && bytes[pos - 1 - escapes] == escape) {
				escapes++;
			}
			if (escapes % 2 == 0 && (escapes < pos - floor || scan_starts)) {
				return ReadBufferEnd(bytes, pos, size, CSVState::STANDARD, state_machine, range_size, range_states);
			}
			// the escapes before this line break are read as part of its line
			pos -= escapes;
		}
	}
	if (scan_starts) {
		// the scanner skips a byte order mark when it reads the first line of the file itself
		idx_t from = MinValue<idx_t>(scan_start, size);
		if (from == 0 &&
		    state_machine.dialect_options.skip_rows.GetValue() + state_machine.dialect_options.header.GetValue() == 0) {
			StringUtil::SkipBOM(bytes, size, from);
		}
		return ReadBufferEnd(bytes, from, size, CSVState::NOT_SET, state_machine, range_size, range_states);
	}
	// the line of the bytes starts in a buffer before
	CSVBufferEnd result;
	result.last_byte = size > 0 ? bytes[size - 1] : '\0';
	result.read = true;
	const idx_t run_end = result.last_byte == '\r' ? size - 1 : size;
	idx_t run_start = run_end;
	while (run_start > 0 && bytes[run_start - 1] == escape) {
		run_start--;
	}
	if (quote == '\0') {
		// without an escape at their end the next byte still reads on its own
		result.known = run_start == run_end;
		return result;
	}
	// the escapes at the end are read from the unquoted value the search guesses first, as a field start behind a
	// delimiter or line break
	idx_t from = run_start;
	if (run_start > 0) {
		const auto before = transitions[static_cast<uint8_t>(bytes[run_start - 1])][standard];
		if (before == CSVState::DELIMITER || before == CSVState::RECORD_SEPARATOR ||
		    before == CSVState::CARRIAGE_RETURN) {
			from = run_start - 1;
		}
	}
	CSVStates states;
	states.Initialize(CSVState::STANDARD);
	for (idx_t i = from; i < size; i++) {
		state_machine.Transition(states, bytes[i]);
	}
	result.state = states.states[1];
	// an odd run that escapes line breaks is inert at the end of a comment line, so it settles nothing with comments
	const auto unquoted_escape = static_cast<uint8_t>(CSVState::UNQUOTED_ESCAPE);
	const bool escapes_line_breaks =
	    transitions[static_cast<uint8_t>('\n')][unquoted_escape] != CSVState::RECORD_SEPARATOR;
	const bool ambiguous_run =
	    (run_end - run_start) % 2 == 1 && escapes_line_breaks && options.comment.GetValue() != '\0';
	result.known = run_start > 0 && !ambiguous_run;
	return result;
}

CSVBufferEnd CSVBufferManager::ReadBufferEnd(const char *bytes, const idx_t from, const idx_t size,
                                             const CSVState state, const CSVStateMachine &state_machine) {
	vector<CSVState> range_states;
	return ReadBufferEnd(bytes, from, size, state, state_machine, 0, range_states);
}

CSVBufferEnd CSVBufferManager::ReadBufferEnd(const char *bytes, const idx_t from, const idx_t size,
                                             const CSVState state, const CSVStateMachine &state_machine,
                                             const idx_t range_size, vector<CSVState> &range_states) {
	// the bytes that keep their state are skipped as the scanner skips them, the state at every range start is kept
	const auto &transitions = state_machine.transition_array;
	CSVByteSkipper skipper(transitions.stop_patterns);
	skipper.SetBuffer(const_data_ptr_cast(bytes), size);
	CSVStates states;
	states.Initialize(state);
	idx_t pos = from;
	idx_t next_range = range_size == 0 ? size : (from / range_size + 1) * range_size;
	while (pos < size) {
		if (pos == next_range) {
			D_ASSERT(pos / range_size - 1 < range_states.size());
			range_states[pos / range_size - 1] = states.states[1];
			next_range += range_size;
		}
		const idx_t to_pos = MinValue<idx_t>(next_range, size);
		state_machine.Transition(states, bytes[pos++]);
		switch (states.states[1]) {
		case CSVState::STANDARD:
			skipper.SkipToStop(transitions.skip_standard, to_pos, pos);
			break;
		case CSVState::QUOTED:
			skipper.SkipToStop(transitions.skip_quoted, to_pos, pos);
			break;
		case CSVState::COMMENT:
			skipper.SkipToStop(transitions.skip_comment, to_pos, pos);
			break;
		default:
			break;
		}
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
