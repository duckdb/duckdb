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
	//! The standard library queue.
	std::queue<T, std::deque<T>> q;

public:
	//! Default constructor.
	ConcurrentQueue() = default;
	//! Constructor reserving capacity.
	explicit ConcurrentQueue(size_t capacity) {
		q.reserve(capacity);
	}

	//! Enqueue an item.
	template <typename U>
	bool enqueue(U &&item) {
		q.push(std::forward<U>(item));
		return true;
	}
	//! Try to dequeue an item.
	bool try_dequeue(T &item) {
		if (q.empty()) {
			return false;
		}
		item = std::move(q.front());
		q.pop();
		return true;
	}
	//! Get the size of the queue.
	size_t size_approx() const {
		return q.size();
	}
	//! Dequeues several elements from the queue.
	//! Returns the number of elements dequeued.
	template <typename It>
	size_t try_dequeue_bulk(It itemFirst, size_t max) {
		for (size_t i = 0; i < max; i++) {
			if (!try_dequeue(*itemFirst)) {
				return i;
			}
			itemFirst++;
		}
		return max;
	}
};

} // namespace duckdb_moodycamel

#endif
