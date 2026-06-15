#include "duckdb/main/engine_iterator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

EngineIterator::EngineIterator(ParseIterator parse_iterator)
    : source(make_uniq<ParseIterator>(std::move(parse_iterator))) {
}

EngineIterator::EngineIterator(unique_ptr<SQLStatement> statement) : pending_single(std::move(statement)) {
}

EngineIterator::~EngineIterator() = default;

EngineIterator::EngineIterator(EngineIterator &&) noexcept = default;
EngineIterator &EngineIterator::operator=(EngineIterator &&) noexcept = default;

unique_ptr<SQLStatement> EngineIterator::NextParseStatement(ClientContext &context) {
	if (source) {
		if (!source->Peek(context)) {
			exhausted = true;
			return nullptr;
		}
		return source->GetStatement();
	}
	// Single-statement source: yield the pending statement exactly once.
	if (single_consumed || !pending_single) {
		exhausted = true;
		return nullptr;
	}
	single_consumed = true;
	return std::move(pending_single);
}

bool EngineIterator::Peek(ClientContext &context) {
	// Already buffered from a prior Peek — just report it.
	if (current_statement) {
		return true;
	}
	// Drain any preprocessed leftovers first.
	if (buffer_cursor < buffer.size()) {
		current_statement = std::move(buffer[buffer_cursor++]);
		return true;
	}
	if (exhausted) {
		return false;
	}
	// Pull the next parse-facing statement and preprocess it into one-or-more engine-facing
	// statements. The preprocessor can swallow a statement entirely (empty expansion), in which
	// case we loop and pull the next one.
	while (true) {
		auto stmt = NextParseStatement(context);
		if (!stmt) {
			return false;
		}
		buffer.clear();
		buffer_cursor = 0;
		buffer.push_back(std::move(stmt));
		context.PreprocessStatements(buffer);
		if (buffer.empty()) {
			// Preprocessor swallowed the statement; pull the next one.
			continue;
		}
		current_statement = std::move(buffer[0]);
		buffer_cursor = 1;
		return true;
	}
}

unique_ptr<SQLStatement> EngineIterator::GetStatement() {
	if (!current_statement) {
		return nullptr;
	}
	return std::move(current_statement);
}

} // namespace duckdb
