#include "duckdb/common/http_state.hpp"

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

shared_ptr<HTTPState> HTTPState::TryGetState(ClientContext &context, bool create_on_missing) {
	auto lookup = context.registered_state.find("http_state");

	if (lookup != context.registered_state.end()) {
		return shared_ptr_cast<ClientContextState, HTTPState>(lookup->second);
	}

	if (!create_on_missing) {
		return nullptr;
	}

	auto http_state = make_shared_ptr<HTTPState>();
	context.registered_state["http_state"] = http_state;
	return http_state;
}

shared_ptr<HTTPState> HTTPState::TryGetState(optional_ptr<FileOpener> opener, bool create_on_missing) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		return TryGetState(*client_context, create_on_missing);
	}
	return nullptr;
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
