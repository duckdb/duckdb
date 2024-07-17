#include "http_state.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

CachedFileHandle::CachedFileHandle(shared_ptr<CachedFile> &file_p) {
	// If the file was not yet initialized, we need to grab a lock.
	if (!file_p->initialized) {
		lock = make_uniq<lock_guard<mutex>>(file_p->lock);
	}
	file = file_p;
}

void CachedFileHandle::SetInitialized(idx_t total_size) {
	if (file->initialized) {
		throw InternalException("Cannot set initialized on cached file that was already initialized");
	}
	if (!lock) {
		throw InternalException("Cannot set initialized on cached file without lock");
	}
	file->size = total_size;
	file->initialized = true;
	lock = nullptr;
}

void CachedFileHandle::AllocateBuffer(idx_t size) {
	if (file->initialized) {
		throw InternalException("Cannot allocate a buffer for a cached file that was already initialized");
	}
	file->data = shared_ptr<char>(new char[size], std::default_delete<char[]>());
	file->capacity = size;
}

void CachedFileHandle::GrowBuffer(idx_t new_capacity, idx_t bytes_to_copy) {
	// copy shared ptr to old data
	auto old_data = file->data;
	// allocate new buffer that can hold the new capacity
	AllocateBuffer(new_capacity);
	// copy the old data
	Write(old_data.get(), bytes_to_copy);
}

void CachedFileHandle::Write(const char *buffer, idx_t length, idx_t offset) {
	//! Only write to non-initialized files with a lock;
	D_ASSERT(!file->initialized && lock);
	memcpy(file->data.get() + offset, buffer, length);
}

void HTTPState::Reset() {
	// Reset Counters
	head_count = 0;
	get_count = 0;
	put_count = 0;
	post_count = 0;
	total_bytes_received = 0;
	total_bytes_sent = 0;

	// Reset cached files
	cached_files.clear();
}

shared_ptr<HTTPState> HTTPState::TryGetState(ClientContext &context) {
	return context.registered_state->GetOrCreate<HTTPState>("http_state");
}

shared_ptr<HTTPState> HTTPState::TryGetState(optional_ptr<FileOpener> opener) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		return TryGetState(*client_context);
	}
	return nullptr;
}

void HTTPState::WriteProfilingInformation(std::ostream &ss) {
	string read = "in: " + StringUtil::BytesToHumanReadableString(total_bytes_received);
	string written = "out: " + StringUtil::BytesToHumanReadableString(total_bytes_sent);
	string head = "#HEAD: " + to_string(head_count);
	string get = "#GET: " + to_string(get_count);
	string put = "#PUT: " + to_string(put_count);
	string post = "#POST: " + to_string(post_count);

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││" + QueryProfiler::DrawPadded("HTTPFS HTTP Stats", TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││                                   ││\n";
	ss << "││" + QueryProfiler::DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
}

//! Get cache entry, create if not exists
shared_ptr<CachedFile> &HTTPState::GetCachedFile(const string &path) {
	lock_guard<mutex> lock(cached_files_mutex);
	auto &cache_entry_ref = cached_files[path];
	if (!cache_entry_ref) {
		cache_entry_ref = make_shared_ptr<CachedFile>();
	}
	return cache_entry_ref;
}

} // namespace duckdb
