//===----------------------------------------------------------------------===//
//						 DuckDB
//
// duckdb/parallel/concurrentqueue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#else

#include <cstddef>
#include <deque>
#include <queue>

namespace duckdb_moodycamel {

template <typename T>
class ConcurrentQueue;
template <typename T>
class BlockingConcurrentQueue;

struct ProducerToken {
	//! Constructor
	template <typename T, typename Traits>
	explicit ProducerToken(ConcurrentQueue<T> &);
	//! Constructor
	template <typename T, typename Traits>
	explicit ProducerToken(BlockingConcurrentQueue<T> &);
	//! Constructor
	ProducerToken(ProducerToken &&) {
	}
	//! Is valid token?
	inline bool valid() const {
		return true;
	}
};

template <typename T>
class ConcurrentQueue {
private:
	//! The queue
	std::queue<T, std::deque<T>> q;

public:
	//! Constructor
	ConcurrentQueue() = default;
	//! Constructor
	explicit ConcurrentQueue(size_t capacity) {
		q.reserve(capacity);
	}

	//! Enqueue item
	template <typename U>
	bool enqueue(U &&item) {
		q.push(std::forward<U>(item));
		return true;
	}
	//! Try to dequeue an item
	bool try_dequeue(T &item) {
		if (q.empty()) {
			return false;
		}
		item = std::move(q.front());
		q.pop();
		return true;
	}
};

} // namespace duckdb_moodycamel

#endif
