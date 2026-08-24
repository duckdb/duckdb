#include "duckdb/main/statement_iterator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

StatementIterator::StatementIterator(ParseIterator &&parse_iterator)
    : source(std::move(parse_iterator)), context(source.GetClientContext()) {
}

StatementIterator::~StatementIterator() = default;

StatementIterator::StatementIterator(StatementIterator &&) noexcept = default;

bool StatementIterator::Peek() {
	// More buffered engine statements from the current peel's expansion?
	if (buffer_cursor < buffer.size()) {
		return true;
	}
	// Otherwise, is there another parse-facing statement to pull? Parses ahead, does NOT preprocess
	// — safe to use as a lookahead.
	return source.Peek();
}

bool StatementIterator::HasMore() {
	// Buffered engine statements from the current peel still remain?
	if (buffer_cursor < buffer.size()) {
		return true;
	}
	// Otherwise defer to the parse-facing source's grammar-free existence check.
	return source.HasMore();
}

unique_ptr<SQLStatement> StatementIterator::GetStatementInternal(optional_ptr<ClientContextLock> lock) {
	// Drain the current peel's expansion first.
	if (buffer_cursor < buffer.size()) {
		return std::move(buffer[buffer_cursor++]);
	}
	// Pull the next parse-facing statement.
	if (!source.Peek()) {
		return nullptr; // exhausted
	}
	auto stmt = source.GetStatement();
	buffer.clear();
	buffer_cursor = 0;
	buffer.push_back(std::move(stmt));
	// Preprocess the peel into one-or-more engine-facing statements. This runs in Get (not Peek) so it
	// sees the transaction state left by the previously executed statement.
	context.PreprocessStatements(buffer, lock);
	if (buffer.empty()) {
		// Preprocessing swallowed the peel — caller skips with `continue`; the next Get pulls on.
		return nullptr;
	}
	buffer_cursor = 1;
	return std::move(buffer[0]);
}

unique_ptr<SQLStatement> StatementIterator::GetStatement() {
	return GetStatementInternal(nullptr);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementWithLock(ClientContextLock &lock) {
	return GetStatementInternal(&lock);
}

} // namespace duckdb
