#include "duckdb/main/statement_iterator.hpp"

#include "duckdb/common/enums/current_transaction_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/statement_preprocessor.hpp"

namespace duckdb {

StatementIterator::StatementIterator(ParseIterator &&parse_iterator) : source(std::move(parse_iterator)) {
}

StatementIterator::~StatementIterator() = default;

StatementIterator::StatementIterator(StatementIterator &&) noexcept = default;
StatementIterator &StatementIterator::operator=(StatementIterator &&) noexcept = default;

bool StatementIterator::Peek(ClientContext &context) {
	// More buffered engine statements from the current peel's expansion?
	if (buffer_cursor < buffer.size()) {
		return true;
	}
	// Otherwise, is there another parse-facing statement to pull? Parses ahead, does NOT preprocess
	// — safe to use as a lookahead.
	return source.Peek(context);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementInternal(ClientContext &context,
                                                              optional_ptr<ClientContextLock> lock) {
	// Drain the current peel's expansion first.
	if (buffer_cursor < buffer.size()) {
		return std::move(buffer[buffer_cursor++]);
	}
	// Pull the next parse-facing statement.
	if (!source.Peek(context)) {
		return nullptr; // exhausted
	}
	auto stmt = source.GetStatement();
	buffer.clear();
	buffer_cursor = 0;
	buffer.push_back(std::move(stmt));
	// Preprocess the peel into one-or-more engine-facing statements. This runs in Get (not Peek) so it
	// sees the transaction state left by the previously executed statement.
	StatementPreprocessor preprocessor(context);
	const CurrentTransactionState transaction_state =
	    context.transaction.HasActiveTransaction() ? IN_ACTIVE_TRANSACTION : NOT_IN_ACTIVE_TRANSACTION;
	if (lock) {
		preprocessor.Preprocess(*lock, buffer, transaction_state);
	} else {
		// No caller-held lock (e.g. the shell): acquire one ourselves for the preprocess pass.
		auto own_lock = context.LockContext();
		preprocessor.Preprocess(*own_lock, buffer, transaction_state);
	}
	if (buffer.empty()) {
		// Preprocessing swallowed the peel — caller skips with `continue`; the next Get pulls on.
		return nullptr;
	}
	buffer_cursor = 1;
	return std::move(buffer[0]);
}

unique_ptr<SQLStatement> StatementIterator::GetStatement(ClientContext &context) {
	return GetStatementInternal(context, nullptr);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementWithLock(ClientContext &context, ClientContextLock &lock) {
	return GetStatementInternal(context, &lock);
}

} // namespace duckdb
