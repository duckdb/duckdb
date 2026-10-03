//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_status.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {
class ClientContext;

//! The status messages of a client context: what its statement is doing, for progress displays. Messages are scoped
//! (see ClientStatus) and can nest - the most recently started one that is still active is displayed, next to the
//! progress bar while the query executes, or on its own before (e.g. while binding) or without one
class ClientStatusState : public ClientContextState {
public:
	static shared_ptr<ClientStatusState> Get(ClientContext &context);

	//! Adds a message, returns its id
	idx_t Push(string message);
	void Replace(idx_t id, string message);
	//! Removes a message - when its scope fails (an exception unwinds through it), the active messages are kept as
	//! the context of the failure (see GetFailureContext)
	void Pop(idx_t id, bool failed);

	//! The progress bar display of the executing query, which shows the message next to the progress
	void RegisterQueryDisplay(ProgressBarDisplay &display);
	void UnregisterQueryDisplay(ProgressBarDisplay &display);
	//! Lock to hold while calling the registered query display
	mutex &DisplayLock() {
		return lock;
	}

	//! The active messages when the last statement failed inside a scope ("A › B"), empty if none
	string GetFailureContext();
	void ClearFailureContext();

	using ClientContextState::QueryEnd;
	void QueryEnd(ClientContext &context) override;

private:
	//! Shows the current message on the query display, or on a display of its own when there is none. Unless forced,
	//! only when the message changed
	void UpdateDisplay(unique_lock<mutex> &guard, bool force);
	string CurrentMessage() const;
	string JoinedMessages() const;

private:
	ClientContext *context = nullptr;
	mutex lock;
	//! The active messages, in the order they were started
	vector<pair<idx_t, string>> messages;
	idx_t next_id = 0;
	//! The query's progress bar display, if any
	optional_ptr<ProgressBarDisplay> query_display;
	//! A display created to show a message while there is no query display
	unique_ptr<ProgressBarDisplay> status_display;
	//! The message currently displayed
	string displayed_message;
	string failure_context;
};

//! Reports what the statement is doing for its lifetime - e.g. "Installing extension httpfs" - on the progress
//! display. Use it around operations that may take a while (network, provisioning): the message is shown right away
class ClientStatus {
public:
	DUCKDB_API ClientStatus(ClientContext &context, string message);
	DUCKDB_API ~ClientStatus();

	//! Replaces the message of this scope (e.g. a wait that changes state)
	DUCKDB_API void Update(string message);

	ClientStatus(const ClientStatus &) = delete;
	ClientStatus &operator=(const ClientStatus &) = delete;

private:
	shared_ptr<ClientStatusState> state;
	idx_t id;
	//! Uncaught exceptions at construction, to tell an unwinding destruction apart
	int uncaught_exceptions;
};

} // namespace duckdb
