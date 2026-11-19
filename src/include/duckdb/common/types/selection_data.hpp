//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <limits>

namespace duckdb {

//! Shared ownership of selection indices, independent of the vectors referencing them.
class SelectionData {
private:
	struct Buffer {
		explicit Buffer(idx_t count);

		atomic<uint32_t> references {1};
		AllocatedData allocation;
	};

public:
	SelectionData() = default;
	SelectionData(std::nullptr_t) { // NOLINT: allow clearing an owning selection
	}
	DUCKDB_API static SelectionData Allocate(idx_t count);
	SelectionData(const SelectionData &other) noexcept : buffer(other.buffer) {
		AddReference();
	}
	SelectionData(SelectionData &&other) noexcept : buffer(other.buffer) {
		other.buffer = nullptr;
	}
	~SelectionData() {
		Reset();
	}
	SelectionData &operator=(const SelectionData &other) noexcept {
		if (buffer.get() != other.buffer.get()) {
			SelectionData replacement(other);
			Swap(replacement);
		}
		return *this;
	}
	SelectionData &operator=(SelectionData &&other) noexcept {
		SelectionData replacement(std::move(other));
		Swap(replacement);
		return *this;
	}

	explicit operator bool() const {
		return bool(buffer);
	}
	data_ptr_t GetData() {
		return buffer->allocation.get();
	}
	idx_t GetSize() const {
		return buffer->allocation.GetSize();
	}
	idx_t GetReferenceCount() const {
		return buffer ? buffer->references.load(std::memory_order_relaxed) : 0;
	}
	void Reset() noexcept {
		auto old = buffer;
		buffer = nullptr;
		if (!old) {
			return;
		}
		// There are no weak references, so a sole owner needs no atomic decrement.
		if (old->references.load(std::memory_order_acquire) == 1) {
			delete old.get();
			return;
		}
		auto previous = old->references.fetch_sub(1, std::memory_order_release);
		D_ASSERT(previous > 0);
		if (previous == 1) {
			std::atomic_thread_fence(std::memory_order_acquire);
			delete old.get();
		}
	}

private:
	void AddReference() noexcept {
		if (buffer) {
			auto previous = buffer->references.fetch_add(1, std::memory_order_relaxed);
			D_ASSERT(previous > 0 && previous < std::numeric_limits<uint32_t>::max());
			(void)previous;
		}
	}
	void Swap(SelectionData &other) noexcept {
		std::swap(buffer, other.buffer);
	}

private:
	optional_ptr<Buffer> buffer;
};

} // namespace duckdb
