//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/condition_variable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_handle.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state.hpp"

namespace duckdb {
class CSVBuffer;
class CSVStateMachine;

//! Residency of a buffer
enum class CSVBufferResidency : uint8_t {
	IN_MEMORY,  //! The buffer is loaded
	NEEDS_LOAD, //! The buffer is not materialized or was evicted
	END_OF_FILE //! The buffer position lies past the end of the file
};

//! What a scanner starting behind a buffer end needs to know about the bytes before it
struct CSVBufferEnd {
	//! The last byte, 0 when there are no bytes
	char last_byte = '\0';
	//! The state the bytes leave the state machine in, the state of their last escapes read from an unquoted value
	//! when not exact
	CSVState state = CSVState::STANDARD;
	//! Whether the state was read from the scan start, on from an exact end, or without quotes from a line break,
	//! so that the bytes behind it read on from it
	bool exact = false;
	//! Whether the state settles how the byte behind the bytes reads, bytes of unknown state ending in an escape do not
	bool known = false;
	//! Whether the end was read
	bool read = false;
	//! Whether a thread reads the end right now
	bool reading = false;
};

//! This class is used to manage the CSV buffers.  Buffers are cached when used for auto detection.
//! When parsing, buffer are not cached and just returned.
//! A CSV Buffer Manager is created for each separate CSV File.
class CSVBufferManager {
public:
	CSVBufferManager(ClientContext &context, const CSVReaderOptions &options, const OpenFileInfo &file,
	                 bool per_file_single_threaded, unique_ptr<CSVFileHandle> file_handle);
	virtual ~CSVBufferManager() = default;

	//! Opens the file and creates the buffer manager matching its layout
	static shared_ptr<CSVBufferManager> Open(ClientContext &context, const CSVReaderOptions &options,
	                                         const OpenFileInfo &file, bool per_file_single_threaded = false,
	                                         unique_ptr<CSVFileHandle> file_handle = nullptr);

	//! Returns a buffer from a buffer id (starting from 0). If it's in the auto-detection then we cache new buffers
	//! Otherwise we remove them from the cache if they are already there, or just return them bypassing the cache.
	virtual shared_ptr<CSVBufferHandle> GetBuffer(const idx_t buffer_idx) = 0;
	//! Pins and returns the buffer if that requires no I/O, otherwise only reports the buffer's residency
	virtual CSVBufferResidency GetBufferResidency(const idx_t buffer_idx, shared_ptr<CSVBufferHandle> &handle) = 0;
	virtual void ResetBuffer(const idx_t buffer_idx) = 0;
	//! If this buffer manager is done. In the context of a buffer manager it means that it read all buffers at least
	//! once.
	virtual bool Done() const = 0;
	//! Resets the buffer manager so the file can be scanned again from the top (e.g., a recursive CTE rescan)
	virtual void ResetBufferManager() = 0;

	//! unique_ptr to the file handle, gets stolen after sniffing
	unique_ptr<CSVFileHandle> file_handle;

	//! Whether buffer byte ranges follow deterministically from the file size
	bool HasKnownBufferRanges() const;
	//! For files with known buffer ranges, the number of buffers in the file
	idx_t KnownBufferCount() const;
	//! For files with known buffer range, the actual size of a given buffer
	idx_t KnownBufferSize(const idx_t buffer_idx) const;

	//! Returns the buffer size set for this CSV buffer manager
	idx_t GetBufferSize() const;
	//! Returns the number of buffers in the cached_buffers cache
	idx_t BufferCount() const;
	//! Reads the ends of the buffers handed out from now on and the states at the range starts inside them, the scan
	//! starting at a row start of the given buffer
	void ReadBufferEnds(shared_ptr<CSVStateMachine> state_machine, idx_t range_size, idx_t scan_start_buffer,
	                    idx_t scan_start_pos);
	//! The end of a buffer, false while buffer ends are not read, refined through inexact ends before it when asked to
	bool GetBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end, bool through_inexact = false);
	//! The exact state at a range start inside a buffer, false when the buffer end was not read exactly
	bool GetRangeStart(idx_t buffer_idx, idx_t buffer_pos, CSVBufferEnd &range_start, bool through_inexact = false);
	//! Where the scan starts inside the buffer, INVALID_INDEX when it starts in another buffer
	idx_t ScanStartIn(idx_t buffer_idx);
	//! Whether buffer ends can be read exactly under the dialect, which takes an escape that works outside quotes
	static bool ReadsExactEnds(const CSVStateMachineOptions &options);
	//! How the bytes before size read on, scan_start where the scan starts inside them, else INVALID_INDEX
	static CSVBufferEnd ScanBufferEnd(const char *bytes, idx_t size, idx_t scan_start,
	                                  const CSVStateMachine &state_machine);
	//! The same, keeping the state at every range start an exact reading passes
	static CSVBufferEnd ScanBufferEnd(const char *bytes, idx_t size, idx_t scan_start,
	                                  const CSVStateMachine &state_machine, idx_t range_size,
	                                  vector<CSVState> &range_states);
	//! How the bytes from `from` before size read on, read from the state before them
	static CSVBufferEnd ReadBufferEnd(const char *bytes, idx_t from, idx_t size, CSVState state,
	                                  const CSVStateMachine &state_machine);
	//! The same, keeping the state at every range start behind `from`
	static CSVBufferEnd ReadBufferEnd(const char *bytes, idx_t from, idx_t size, CSVState state,
	                                  const CSVStateMachine &state_machine, idx_t range_size,
	                                  vector<CSVState> &range_states);

	string GetFilePath() const;

	bool IsBlockUnloaded(idx_t block_idx);

	ClientContext &context;
	bool sniffing = false;
	const bool per_file_single_threaded;

protected:
	//! The file this Buffer Manager refers to
	const OpenFileInfo file;
	//! Reads the end of a buffer when it is handed out, without holding the mutex
	void RememberBufferEnd(idx_t buffer_idx, const CSVBufferHandle &handle);
	//! Hands the buffer and those before it without an exact end out in order, so that their ends are read on, back
	//! through ends that were read without being exact only when asked to
	void ReadBufferEndsUpTo(idx_t buffer_idx, bool through_inexact);
	//! The end of a buffer when it was read, after a read in flight
	bool LookupBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end);
	//! The range starts inside a buffer
	idx_t RangeStatesPerBuffer() const;
	//! The cached buffers
	vector<shared_ptr<CSVBuffer>> cached_buffers;
	//! The ends of the buffers handed out, which outlive the buffers themselves
	vector<CSVBufferEnd> buffer_ends;
	//! The states at the range starts inside the buffers whose ends are exact, NOT_SET where not read
	vector<CSVState> range_states;
	//! The state machine the buffer ends are read with, unset while they are not read
	shared_ptr<CSVStateMachine> buffer_end_state_machine;
	//! The bytes of a range, 0 while buffer ends are not read
	idx_t range_size = 0;
	//! The buffer and the position in it where the scan starts
	idx_t scan_start_buffer = 0;
	idx_t scan_start_pos = 0;
	//! Whether the dialect the ends are read with lets them be exact
	bool exact_ends = false;
	//! Signals the end of a read in flight
	condition_variable buffer_end_read;
	//! The size of the buffer, if the csv file has a smaller size than this, we will use that instead to malloc less
	idx_t buffer_size;
	//! Because the buffer manager can be accessed in Parallel we need a mutex.
	mutable mutex main_mutex;
};

} // namespace duckdb
