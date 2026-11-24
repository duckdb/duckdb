//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
	//! The state the bytes leave the state machine in
	CSVState state = CSVState::STANDARD;
	//! False when the line of the last bytes starts before them, which leaves their state unknown
	bool known = false;
	//! Whether the end was read
	bool read = false;
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
	//! Reads how every buffer handed out from now on ends, for the scanners that start behind a buffer end
	void ReadBufferEnds(shared_ptr<CSVStateMachine> state_machine);
	//! The end of a buffer as read when it was handed out, false while buffer ends are not read
	bool GetBufferEnd(idx_t buffer_idx, CSVBufferEnd &buffer_end);
	//! How the bytes before size read on, file_start when the bytes start the file
	static CSVBufferEnd ScanBufferEnd(const char *bytes, idx_t size, bool file_start,
	                                  const CSVStateMachine &state_machine);
	//! How the bytes from `from` before size read on, read from the state before them
	static CSVBufferEnd ReadBufferEnd(const char *bytes, idx_t from, idx_t size, CSVState state,
	                                  const CSVStateMachine &state_machine);

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
	//! The cached buffers
	vector<shared_ptr<CSVBuffer>> cached_buffers;
	//! The ends of the buffers handed out, which outlive the buffers themselves
	vector<CSVBufferEnd> buffer_ends;
	//! The state machine the buffer ends are read with, unset while they are not read
	shared_ptr<CSVStateMachine> buffer_end_state_machine;
	//! The size of the buffer, if the csv file has a smaller size than this, we will use that instead to malloc less
	idx_t buffer_size;
	//! Because the buffer manager can be accessed in Parallel we need a mutex.
	mutable mutex main_mutex;
};

} // namespace duckdb
