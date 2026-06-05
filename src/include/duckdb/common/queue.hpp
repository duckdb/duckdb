//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/memory_safety.hpp"
#include <queue>

namespace duckdb {

template <class DATA_TYPE, class CONTAINER = std::deque<DATA_TYPE>, bool SAFE = true>
class queue : public std::queue<DATA_TYPE, CONTAINER> { // NOLINT: matching name of std
public:
	using original = std::queue<DATA_TYPE, CONTAINER>;
	using original::original;
	using container_type = typename original::container_type;
	using value_type = typename original::value_type;
	using size_type = typename container_type::size_type;
	using reference = typename container_type::reference;
	using const_reference = typename container_type::const_reference;

public:
	// Because we create the other constructor, the implicitly created constructor
	// gets deleted, so we have to be explicit
	queue() = default;
	queue(original &&other) : original(std::move(other)) { // NOLINT: allow implicit conversion
	}
	template <bool INTERNAL_SAFE>
	queue(queue<DATA_TYPE, CONTAINER, INTERNAL_SAFE> &&other) : original(std::move(other)) { // NOLINT
	}

	inline void clear() noexcept {
		original::c.clear();
	}

	reference front() {
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'front' called on an empty queue!");
		}
		return original::front();
	}

	const_reference front() const {
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'front' called on an empty queue!");
		}
		return original::front();
	}

	reference back() {
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'back' called on an empty queue!");
		}
		return original::back();
	}

	const_reference back() const {
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'back' called on an empty queue!");
		}
		return original::back();
	}

	void pop() {
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'pop' called on an empty queue!");
		}
		original::pop();
	}
};

template <typename T, typename Container = std::deque<T>>
using unsafe_queue = queue<T, Container, false>;

} // namespace duckdb
