#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class CachedFileHandle;

//! Represents a file that is intended to be fully downloaded, then used in parallel by multiple threads
class CachedFile : public enable_shared_from_this<CachedFile> {
	friend class CachedFileHandle;

public:
	unique_ptr<CachedFileHandle> GetHandle() {
		auto this_ptr = shared_from_this();
		return make_uniq<CachedFileHandle>(this_ptr);
	}

private:
	//! Cached Data
	shared_ptr<char> data;
	//! Data capacity
	uint64_t capacity = 0;
	//! Size of file
	idx_t size;
	//! Lock for initializing the file
	mutex lock;
	//! When initialized is set to true, the file is safe for parallel reading without holding the lock
	atomic<bool> initialized = {false};
};

//! Handle to a CachedFile
class CachedFileHandle {
public:
	explicit CachedFileHandle(shared_ptr<CachedFile> &file_p);

	//! allocate a buffer for the file
	void AllocateBuffer(idx_t size);
	//! Indicate the file is fully downloaded and safe for parallel reading without lock
	void SetInitialized(idx_t total_size);
	//! Grow buffer to new size, copying over `bytes_to_copy` to the new buffer
	void GrowBuffer(idx_t new_capacity, idx_t bytes_to_copy);
	//! Write to the buffer
	void Write(const char *buffer, idx_t length, idx_t offset = 0);

	bool Initialized() {
		return file->initialized;
	}
	const char *GetData() {
		return file->data.get();
	}
	uint64_t GetCapacity() {
		return file->capacity;
	}
	//! Return the size of the initialized file
	idx_t GetSize() {
		D_ASSERT(file->initialized);
		return file->size;
	}

private:
	unique_ptr<lock_guard<mutex>> lock;
	shared_ptr<CachedFile> file;
};

class HTTPState : public ClientContextState {
public:
	//! Reset all counters and cached files
	void Reset();
	//! Get cache entry, create if not exists
	shared_ptr<CachedFile> &GetCachedFile(const string &path);
	//! Helper functions to get the HTTP state
	static shared_ptr<HTTPState> TryGetState(ClientContext &context);
	static shared_ptr<HTTPState> TryGetState(optional_ptr<FileOpener> opener);

	bool IsEmpty() {
		return head_count == 0 && get_count == 0 && put_count == 0 && post_count == 0 && total_bytes_received == 0 &&
		       total_bytes_sent == 0;
	}

	atomic<idx_t> head_count {0};
	atomic<idx_t> get_count {0};
	atomic<idx_t> put_count {0};
	atomic<idx_t> post_count {0};
	atomic<idx_t> total_bytes_received {0};
	atomic<idx_t> total_bytes_sent {0};

	//! Called by the ClientContext when the current query ends
	void QueryEnd(ClientContext &context) override {
		Reset();
	}
	void WriteProfilingInformation(std::ostream &ss) override;

private:
	//! Mutex to lock when getting the cached file(Parallel Only)
	mutex cached_files_mutex;
	//! In case of fully downloading the file, the cached files of this query
	unordered_map<string, shared_ptr<CachedFile>> cached_files;
};

} // namespace duckdb
