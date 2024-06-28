//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/prepared_statement_mode.hpp"

namespace duckdb {
class ClientContext;
class ErrorData;
class MetaTransaction;
class PreparedStatementData;
class SQLStatement;
struct PendingQueryParameters;

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
};

} // namespace duckdb
