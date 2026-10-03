#include "duckdb/main/client_status.hpp"

#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

#include <exception>

namespace duckdb {

shared_ptr<ClientStatusState> ClientStatusState::Get(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<ClientStatusState>("client_status");
	lock_guard<mutex> guard(state->lock);
	if (!state->context) {
		// the context whose progress display shows the messages - for a shared state, the one it was shared from
		state->context = &context;
	}
	return state;
}

void ClientStatusState::ShareWith(ClientContext &internal_context, ClientContext &context) {
	auto state = Get(context);
	internal_context.registered_state->Remove("client_status");
	internal_context.registered_state->Insert("client_status", std::move(state));
}

idx_t ClientStatusState::Push(string message) {
	unique_lock<mutex> guard(lock);
	auto id = next_id++;
	messages.emplace_back(id, std::move(message));
	UpdateDisplay(guard, false);
	return id;
}

void ClientStatusState::Replace(idx_t id, string message) {
	unique_lock<mutex> guard(lock);
	for (auto &entry : messages) {
		if (entry.first == id) {
			entry.second = std::move(message);
			break;
		}
	}
	// redraw even when the message is the same - a repeated update keeps the elapsed time moving
	UpdateDisplay(guard, true);
}

void ClientStatusState::Pop(idx_t id, bool failed) {
	unique_lock<mutex> guard(lock);
	if (failed && failure_context.empty()) {
		// the innermost failing scope sees all messages that were active when the failure happened
		failure_context = JoinedMessages();
	}
	for (idx_t i = 0; i < messages.size(); i++) {
		if (messages[i].first == id) {
			messages.erase(messages.begin() + static_cast<int64_t>(i));
			break;
		}
	}
	UpdateDisplay(guard, false);
}

void ClientStatusState::RegisterQueryDisplay(ProgressBarDisplay &display) {
	unique_lock<mutex> guard(lock);
	if (status_display) {
		// the query display takes over - remove the message from the display of its own
		status_display->UpdateStatus(string());
		status_display.reset();
	}
	query_display = &display;
	displayed_message.clear();
	UpdateDisplay(guard, false);
}

void ClientStatusState::UnregisterQueryDisplay(ProgressBarDisplay &display) {
	unique_lock<mutex> guard(lock);
	if (query_display.get() != &display) {
		return;
	}
	query_display = nullptr;
	displayed_message.clear();
	UpdateDisplay(guard, false);
}

string ClientStatusState::GetFailureContext() {
	lock_guard<mutex> guard(lock);
	return failure_context;
}

void ClientStatusState::ClearFailureContext() {
	lock_guard<mutex> guard(lock);
	failure_context.clear();
}

void ClientStatusState::QueryEnd(ClientContext &context) {
	unique_lock<mutex> guard(lock);
	if (messages.empty() && status_display) {
		status_display->UpdateStatus(string());
		status_display.reset();
		displayed_message.clear();
	}
}

string ClientStatusState::CurrentMessage() const {
	return messages.empty() ? string() : messages.back().second;
}

string ClientStatusState::JoinedMessages() const {
	string result;
	for (auto &entry : messages) {
		if (!result.empty()) {
			result += " \xE2\x80\xBA "; // " › "
		}
		result += entry.second;
	}
	return result;
}

void ClientStatusState::UpdateDisplay(unique_lock<mutex> &guard, bool force) {
	auto message = CurrentMessage();
	if (!force && message == displayed_message) {
		return;
	}
	displayed_message = message;
	if (query_display) {
		query_display->UpdateStatus(message);
		return;
	}
	if (message.empty()) {
		if (status_display) {
			status_display->UpdateStatus(message);
			status_display.reset();
		}
		return;
	}
	if (!status_display) {
		// no query is executing (yet) - show the message on a display of its own, if progress is displayed at all
		if (!context) {
			return;
		}
		auto &config = ClientConfig::GetConfig(*context);
		if (!config.enable_progress_bar || !config.print_progress_bar) {
			return;
		}
		auto create_display =
		    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		status_display = create_display();
		if (!status_display) {
			return;
		}
	}
	status_display->UpdateStatus(message);
}

ClientStatus::ClientStatus(ClientContext &context, string message)
    : state(ClientStatusState::Get(context)), uncaught_exceptions(std::uncaught_exceptions()) {
	id = state->Push(std::move(message));
}

ClientStatus::~ClientStatus() {
	try {
		state->Pop(id, std::uncaught_exceptions() > uncaught_exceptions);
	} catch (...) { // LCOV_EXCL_START
		// a display failure must not escape a destructor
	} // LCOV_EXCL_STOP
}

void ClientStatus::Update(string message) {
	state->Replace(id, std::move(message));
}

} // namespace duckdb
