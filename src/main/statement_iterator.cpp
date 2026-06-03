#include "duckdb/main/statement_iterator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/connect_execute_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/passthrough_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

StatementIterator::StatementIterator(string sql_p) : sql(std::move(sql_p)) {
	// Mirror Parser::ParseQuery's up-front normalization: reject invalid UTF-8 (otherwise the
	// tokenizer can recurse on bad bytes — see ossfuzz clusterfuzz-test-24) and strip non-ASCII
	// Unicode spaces.
	Parser::ValidateUTF8Query(sql);
	string normalized;
	if (Parser::StripUnicodeSpaces(sql, normalized)) {
		sql = std::move(normalized);
	}
}

StatementIterator::~StatementIterator() = default;

StatementIterator::StatementIterator(StatementIterator &&) noexcept = default;
StatementIterator &StatementIterator::operator=(StatementIterator &&) noexcept = default;

void StatementIterator::EnablePreprocessing() {
	preprocess_on_peek = true;
}

bool StatementIterator::Peek(ClientContext &context) {
	// Already buffered from a prior Peek — just report it.
	if (current_statement) {
		return true;
	}
	// Drain any preprocessed leftovers first.
	if (preprocess_buffer_cursor < preprocess_buffer.size()) {
		current_statement = std::move(preprocess_buffer[preprocess_buffer_cursor++]);
		return true;
	}
	if (exhausted) {
		return false;
	}
	// Charge the time spent tokenizing/parsing on this Peek to MetricParserTotalTime so callers
	// (Query, ParseStatementsInternal, ParseStatementRaw, …) get parse metrics without each
	// having to remember to wrap us in a timer.
	auto parser_timer = QueryProfiler::Get(context).StartTimer<MetricParserTotalTime>();
	auto options = context.GetParserOptions();
	// On the very first Peek, give `parser_override` extensions a chance to claim the whole
	// query. If one does, we yield its statements one at a time and skip the PEG path entirely.
	if (!override_resolved) {
		override_resolved = true;
		if (options.extensions) {
			bool has_strict_extension_error = false;
			ErrorData last_strict_extension_error;
			for (auto &ext : options.extensions->ParserExtensions()) {
				if (!ext.parser_override) {
					continue;
				}
				if (options.parser_override_setting == AllowParserOverride::DEFAULT_OVERRIDE) {
					continue;
				}
				auto result = ext.parser_override(ext.parser_info.get(), sql, options);
				if (result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
					overridden_statements = make_uniq<vector<unique_ptr<SQLStatement>>>(std::move(result.statements));
					break;
				}
				if (options.parser_override_setting == AllowParserOverride::STRICT_OVERRIDE) {
					if (result.type == ParserExtensionResultType::DISPLAY_EXTENSION_ERROR) {
						has_strict_extension_error = true;
						last_strict_extension_error = std::move(result.error);
					} else {
						has_strict_extension_error = false;
					}
					continue;
				}
			}
			if (!overridden_statements && options.parser_override_setting == AllowParserOverride::STRICT_OVERRIDE &&
			    has_strict_extension_error) {
				last_strict_extension_error.Throw();
			}
		}
	}
	if (overridden_statements) {
		if (override_cursor >= overridden_statements->size()) {
			exhausted = true;
			return false;
		}
		current_statement = std::move((*overridden_statements)[override_cursor++]);
		return true;
	}
	if (!parser) {
		parser = make_uniq<Parser>(options);
	}
	if (!tokens) {
		// Tokenize the full input once. Subsequent Peek calls walk through `tokens` via
		// `token_cursor`; we never re-tokenize.
		tokens = make_uniq<vector<MatcherToken>>();
		ParserTokenizer tokenizer(sql, *tokens);
		tokenizer.TokenizeInput();
	}
	// Walk the token cursor through the cached `tokens`, calling Parser::ParseTopLevelStatement
	// repeatedly. A nullptr return with cursor advanced means a separator-only TopLevelStatement
	// (e.g. between statements or trailing ';'s); we loop past it. A nullptr return with cursor
	// at end means the input is exhausted.
	auto at_end_of_real_tokens = [&]() {
		return token_cursor >= tokens->size() || (*tokens)[token_cursor].type == TokenType::END_OF_INPUT;
	};
	while (true) {
		if (at_end_of_real_tokens()) {
			exhausted = true;
			return false;
		}
		unique_ptr<SQLStatement> stmt;
		try {
			stmt = parser->ParseTopLevelStatement(*tokens, token_cursor);
		} catch (ParserException &) {
			// Mirror Parser::ParseQuery's parse_function-extension fallback so extensions like
			// `quack` can claim a segment that PEG couldn't parse.
			stmt = parser->TryParseExtensionStatement(*tokens, token_cursor, sql);
			if (!stmt) {
				throw;
			}
		}
		if (stmt) {
			// ParseTopLevelStatement doesn't populate stmt->query (it operates on tokens, not the
			// source string). Mirror Parser::ParseQuery's per-statement post-processing: span from
			// the statement's start to the next statement's start (or end of input) so the trailing
			// `;` and inter-statement whitespace end up inside stmt->query — downstream consumers
			// (logging, error reporting, EXPLAIN) rely on that shape.
			idx_t stmt_loc = stmt->stmt_location;
			idx_t end_loc = sql.size();
			if (token_cursor < tokens->size() && (*tokens)[token_cursor].type != TokenType::END_OF_INPUT) {
				end_loc = (*tokens)[token_cursor].offset;
			}
			stmt->query = sql.substr(stmt_loc, end_loc - stmt_loc);
			stmt->stmt_location = 0;
			stmt->stmt_length = stmt->query.size();
			if (stmt->type == StatementType::CREATE_STATEMENT) {
				auto &create = stmt->Cast<CreateStatement>();
				create.info->sql = stmt->query;
			}
			// CONNECT_EXECUTE marker: stash target, loop, then handle the next peel below.
			if (stmt->type == StatementType::CONNECT_EXECUTE_STATEMENT) {
				auto &marker = stmt->Cast<ConnectExecuteStatement>();
				pending_connect_execute.active = true;
				pending_connect_execute.target = marker.target;
				pending_connect_execute.needs_passthrough = !marker.target_is_local;
				continue;
			}
			// Remote CONNECT … EXECUTE: ship the source bytes verbatim. The local parser already
			// ran for boundary detection but the payload is whatever the remote understands —
			// no local preprocessing.
			if (pending_connect_execute.active && pending_connect_execute.needs_passthrough) {
				auto saved_query = stmt->query;
				auto passthrough =
				    make_uniq<PassthroughStatement>(std::move(pending_connect_execute.target), std::move(saved_query));
				passthrough->query = stmt->query;
				pending_connect_execute = PendingConnectExecute();
				current_statement = std::move(passthrough);
				return true;
			}
			// Everything below runs locally: top-level statement, OR `CONNECT LOCAL EXECUTE …`
			// whose inner runs locally. preprocess_on_peek drives PRAGMA reparse /
			// MULTI_STATEMENT unpack / transaction wrapping the same way the eager API did.
			const bool wrap_as_local_passthrough = pending_connect_execute.active;
			if (wrap_as_local_passthrough) {
				pending_connect_execute = PendingConnectExecute();
			}
			if (preprocess_on_peek) {
				preprocess_buffer.clear();
				preprocess_buffer_cursor = 0;
				preprocess_buffer.push_back(std::move(stmt));
				context.PreprocessStatements(preprocess_buffer);
				if (preprocess_buffer.empty()) {
					continue;
				}
				if (wrap_as_local_passthrough) {
					// Wrap each preprocessed expansion so the chokepoint lets it past the sticky
					// CONNECT binding; bind delegates to the inner.
					for (auto &s : preprocess_buffer) {
						s = make_uniq<PassthroughStatement>(std::move(s));
					}
				}
				current_statement = std::move(preprocess_buffer[0]);
				preprocess_buffer_cursor = 1;
				return true;
			}
			if (wrap_as_local_passthrough) {
				stmt = make_uniq<PassthroughStatement>(std::move(stmt));
			}
			current_statement = std::move(stmt);
			return true;
		}
		if (at_end_of_real_tokens()) {
			if (pending_connect_execute.active) {
				throw ParserException("CONNECT %s EXECUTE expects a following SQL statement",
				                      pending_connect_execute.needs_passthrough ? pending_connect_execute.target
				                                                                : string("LOCAL"));
			}
			exhausted = true;
			return false;
		}
		// separator-only TLS in the middle of the input — loop and try the next.
	}
}

unique_ptr<SQLStatement> StatementIterator::GetStatement() {
	if (!current_statement) {
		return nullptr;
	}
	return std::move(current_statement);
}

} // namespace duckdb
