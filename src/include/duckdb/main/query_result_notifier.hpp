//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result_notifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"

#include <functional>

namespace duckdb {

//! Notifies a waiting stream consumer that the result's observable state may have
//! changed. Notifications might overlap and the state that triggered it may have
//! changed by the time the consumer tries to fetch. The callback runs on an engine
//! thread and so it _must_ be tiny and _must not_ call back into DuckDB. A callback
//! that re-enters DuckDB can deadlock the connection.
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
	//! Callers must not hold any engine lock. The callback runs under the notifier's lock.
	void Notify() {
		lock_guard<mutex> guard(lock);
		if (callback) {
			callback();
		}
	}
	//! Non-blocking notify for signal handlers (ClientContext::Interrupt). On contention
	//! the notification is dropped. That is safe. A contending Notify already wakes the
	//! consumer, and a contending Clear means the result is going away.
	void TryNotify() {
		if (!lock.try_lock()) {
			return;
		}
		if (callback) {
			callback();
		}
		lock.unlock();
	}

private:
	mutex lock;
	notify_callback_t callback;
};

} // namespace duckdb
