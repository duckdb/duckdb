#include "duckdb/main/statement_iterator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

static bool StatementIteratorIsExplainAnalyze(SQLStatement *statement) {
	if (!statement || statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	return statement->Cast<ExplainStatement>().explain_type == ExplainType::EXPLAIN_ANALYZE;
}

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
	if (pending_statement) {
		return true;
	}
	// Otherwise, is there another parse-facing statement to pull? Parses ahead, does NOT preprocess
	// — safe to use as a lookahead.
	parser_timer.Start();
	if (!source.Peek()) {
		parser_timer.Reset();
		return false;
	}
	parser_timer.End();
	pending_statement = source.GetStatement();
	if (!pending_statement) {
		parser_timer.Reset();
		return false;
	}
	return true;
}

bool StatementIterator::HasMore() {
	// Buffered engine statements from the current peel still remain?
	if (buffer_cursor < buffer.size()) {
		return true;
	}
	if (pending_statement) {
		return true;
	}
	// Otherwise defer to the parse-facing source's grammar-free existence check.
	return source.HasMore();
}

unique_ptr<SQLStatement> StatementIterator::GetStatementInternal(optional_ptr<ClientContextLock> lock,
                                                                 bool profile_statement) {
	// Drain the current peel's expansion first.
	if (buffer_cursor < buffer.size()) {
		auto statement = std::move(buffer[buffer_cursor++]);
		if (profile_statement) {
			auto &profiler = QueryProfiler::Get(context);
			profiler.StartQuery(statement->query, StatementIteratorIsExplainAnalyze(statement.get()));
			profiler.AddParserTime(parser_timer);
		}
		parser_timer.Reset();
		return statement;
	}
	// Pull the next parse-facing statement.
	if (!pending_statement && !Peek()) {
		return nullptr; // exhausted
	}
	auto stmt = std::move(pending_statement);
	buffer.clear();
	buffer_cursor = 0;
	buffer.push_back(std::move(stmt));
	// Preprocess the peel into one-or-more engine-facing statements. This runs in Get (not Peek) so it
	// sees the transaction state left by the previously executed statement.
	context.PreprocessStatements(buffer, lock);
	if (buffer.empty()) {
		parser_timer.Reset();
		// Preprocessing swallowed the peel — caller skips with `continue`; the next Get pulls on.
		return nullptr;
	}
	buffer_cursor = 1;
	auto statement = std::move(buffer[0]);
	if (profile_statement) {
		auto &profiler = QueryProfiler::Get(context);
		profiler.StartQuery(statement->query, StatementIteratorIsExplainAnalyze(statement.get()));
		profiler.AddParserTime(parser_timer);
	}
	parser_timer.Reset();
	return statement;
}

unique_ptr<SQLStatement> StatementIterator::GetStatement() {
	return GetStatementInternal(nullptr, false);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementWithLock(ClientContextLock &lock) {
	return GetStatementInternal(&lock, false);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementForExecution() {
	return GetStatementInternal(nullptr, true);
}

unique_ptr<SQLStatement> StatementIterator::GetStatementForExecutionWithLock(ClientContextLock &lock) {
	return GetStatementInternal(&lock, true);
}

} // namespace duckdb
