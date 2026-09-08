//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result_notifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <functional>
#include <thread>

namespace duckdb {

//! Notifies the consumer of a query result when its observable state may have changed: a chunk became
//! available, producers parked waiting for the retention decision, execution finished or failed, or
//! the query was interrupted.
//!
//! The callback runs on an engine thread, and inside a signal handler when the application
//! interrupts from one, so it _must_ be tiny, _must not_ block, and _must never_ call back into
//! DuckDB: participation takes this lock on every executor step, so a blocking callback deadlocks
//! the connection.
//!
//! Notifications may collide or be merged, and carry no arguments. A receiver should always just
//! call Poll or TryFetch, and keep calling while the answer is READY before waiting again.
//! Participating calls (Fetch, ExecuteTask, Collection) never run the callback on the caller's
//! thread.
class QueryResultNotifier {
public:
	using notify_callback_t = std::function<void()>;

	void Set(notify_callback_t callback_p) {
		lock_guard<mutex> guard(lock);
		callback = std::move(callback_p);
	}
	//! After Clear returns the callback is never called again
	void Clear() {
		lock_guard<mutex> guard(lock);
		callback = nullptr;
	}
	//! Callers must not hold any engine lock. The callback runs under the notifier's lock
	void Notify() {
		lock_guard<mutex> guard(lock);
		Run();
	}
	//! Non-blocking notify for signal handlers (ClientContext::Interrupt). On contention the
	//! notification is dropped: a contending Notify already wakes the consumer, and a contending
	//! Clear means the result is going away
	void TryNotify() {
		if (!lock.try_lock()) {
			return;
		}
		Run();
		lock.unlock();
	}

	//! Mark the calling thread as the participating consumer until EndParticipation. Transitions it
	//! causes are self-observed: it sees them in the call's return value
	void BeginParticipation() {
		lock_guard<mutex> guard(lock);
		D_ASSERT(!participating);
		participating = true;
		participating_thread = std::this_thread::get_id();
	}
	void EndParticipation() {
		lock_guard<mutex> guard(lock);
		participating = false;
	}

	//! RAII wrapper around BeginParticipation / EndParticipation. A null notifier is a no-op
	class ParticipationGuard {
	public:
		explicit ParticipationGuard(optional_ptr<QueryResultNotifier> notifier_p) : notifier(notifier_p) {
			if (notifier) {
				notifier->BeginParticipation();
			}
		}
		~ParticipationGuard() {
			if (notifier) {
				notifier->EndParticipation();
			}
		}
		ParticipationGuard(const ParticipationGuard &) = delete;
		ParticipationGuard &operator=(const ParticipationGuard &) = delete;

	private:
		optional_ptr<QueryResultNotifier> notifier;
	};

private:
	//! The caller holds the lock
	void Run() {
		if (!callback || (participating && participating_thread == std::this_thread::get_id())) {
			return;
		}
		// A throwing callback violates the contract, and must not tear down the engine
		try {
			callback();
		} catch (...) { // LCOV_EXCL_LINE
		}
	}

private:
	mutex lock;
	notify_callback_t callback;
	bool participating = false;
	std::thread::id participating_thread;
};

} // namespace duckdb
