//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/prepared_statement_mode.hpp"

namespace duckdb {
class ClientContext;
class ErrorData;
class MetaTransaction;
class PreparedStatementData;
class SQLStatement;
struct PendingQueryParameters;
class RegisteredStateManager;

enum class RebindQueryInfo { DO_NOT_REBIND, ATTEMPT_TO_REBIND };

struct PreparedStatementCallbackInfo {
	PreparedStatementCallbackInfo(PreparedStatementData &prepared_statement, const PendingQueryParameters &parameters)
	    : prepared_statement(prepared_statement), parameters(parameters) {
	}

	PreparedStatementData &prepared_statement;
	const PendingQueryParameters &parameters;
};

//! ClientContextState is virtual base class for ClientContext-local (or Query-Local, using QueryEnd callback) state
//! e.g. caches that need to live as long as a ClientContext or Query.
class ClientContextState {
public:
	virtual ~ClientContextState() = default;
	virtual void QueryBegin(ClientContext &context) {
	}
	virtual void QueryEnd() {
	}
	virtual void QueryEnd(ClientContext &context) {
		QueryEnd();
	}
	virtual void TransactionBegin(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual void TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual void TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	}
	virtual bool CanRequestRebind() {
		return false;
	}
	virtual RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual RebindQueryInfo OnFinalizePrepare(ClientContext &context, PreparedStatementData &prepared_statement,
	                                          PreparedStatementMode mode) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual RebindQueryInfo OnExecutePrepared(ClientContext &context, PreparedStatementCallbackInfo &info,
	                                          RebindQueryInfo current_rebind) {
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	virtual void WriteProfilingInformation(std::ostream &ss) {
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class RegisteredStateManager {
public:
	template <class T, typename... ARGS>
	shared_ptr<T> GetOrCreate(const string &key, ARGS &&... args) {
		lock_guard<mutex> l(lock);
		auto lookup = registered_state.find(key);
		if (lookup != registered_state.end()) {
			return shared_ptr_cast<ClientContextState, T>(lookup->second);
		}
		auto cache = make_shared_ptr<T>(std::forward<ARGS>(args)...);
		registered_state[key] = cache;
		return cache;
	}

	template <class T>
	shared_ptr<T> Get(const string &key) {
		lock_guard<mutex> l(lock);
		auto lookup = registered_state.find(key);
		if (lookup == registered_state.end()) {
			return nullptr;
		}
		return shared_ptr_cast<ClientContextState, T>(lookup->second);
	}

	void Insert(const string &key, shared_ptr<ClientContextState> state_p) {
		lock_guard<mutex> l(lock);
		registered_state.insert(make_pair(key, std::move(state_p)));
	}

	void Remove(const string &key) {
		lock_guard<mutex> l(lock);
		registered_state.erase(key);
	}

	vector<shared_ptr<ClientContextState>> States() {
		lock_guard<mutex> l(lock);
		vector<shared_ptr<ClientContextState>> states;
		for (auto &entry : registered_state) {
			states.push_back(entry.second);
		}
		return states;
	}

private:
	mutex lock;
	unordered_map<string, shared_ptr<ClientContextState>> registered_state;
};

} // namespace duckdb
