#include "duckdb/main/parse_iterator.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/connect_mode/connect_mode_parser.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/passthrough_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

//! Layer-1 connect-mode pre-pass. Returns true and fills `out` only when the input contains a
//! `CONNECT <name> EXECUTE <payload>` form; returns false to let the normal SQL parser handle the
//! whole query unchanged. EXECUTE chunks become a PassthroughStatement carrying the verbatim
//! payload — the SQL parser never sees the payload text, so arbitrary (even non-SQL) payloads
//! survive intact. Every other chunk shape (plain CONNECT/DISCONNECT, malformed control
//! statements, ordinary SQL) is left to the normal parser so its grammar/binder error messages
//! are preserved.
static bool TryParseConnectMode(const ParserOptions &options, const string &query,
                                vector<unique_ptr<SQLStatement>> &out) {
	// Cheap gate: only `CONNECT … EXECUTE` is special. Skip the Layer-1 pass otherwise.
	auto lowered = StringUtil::Lower(query);
	if (!StringUtil::Contains(lowered, "connect") || !StringUtil::Contains(lowered, "execute")) {
		return false;
	}
	vector<ConnectModeChunk> chunks;
	try {
		ConnectModeParser layer1(query);
		chunks = layer1.AllRemaining();
	} catch (...) {
		// Layer-1 couldn't classify the input — let the normal SQL parser produce its (better) error.
		return false;
	}
	bool has_execute = false;
	for (auto &chunk : chunks) {
		if (chunk.type == ConnectModeChunk::Type::EXECUTE) {
			has_execute = true;
			break;
		}
	}
	if (!has_execute) {
		// No EXECUTE form present — let the normal parser handle the whole query (it owns the error
		// messages for plain/malformed CONNECT and DISCONNECT).
		return false;
	}
	for (auto &chunk : chunks) {
		if (chunk.type == ConnectModeChunk::Type::EXECUTE) {
			auto stmt = make_uniq<PassthroughStatement>(chunk.target, chunk.payload);
			stmt->query = chunk.text;
			out.push_back(std::move(stmt));
			continue;
		}
		// Everything else (CONTROL / RAW / malformed): hand the chunk's text to the normal parser.
		Parser parser(options);
		parser.ParseQuery(chunk.text);
		for (auto &s : parser.statements) {
			out.push_back(std::move(s));
		}
	}
	return true;
}

ParseIterator::ParseIterator(ClientContext &context_p, const string &sql_p)
    : context(context_p), sql(Parser::NormalizeSQLString(sql_p)) {
}

ParseIterator::~ParseIterator() = default;

ParseIterator::ParseIterator(ParseIterator &&) noexcept = default;

ClientContext &ParseIterator::GetClientContext() {
	return context;
}

bool ParseIterator::Peek() {
	auto &client_context = context;
	// Already buffered from a prior Peek — just report it.
	if (current_statement) {
		return true;
	}
	if (exhausted) {
		return false;
	}
	// Charge the time spent tokenizing/parsing on this Peek to MetricParserTotalTime so callers
	// get parse metrics without each having to remember to wrap us in a timer.
	auto parser_timer = QueryProfiler::Get(client_context).StartTimer<MetricParserTotalTime>();
	auto options = client_context.GetParserOptions();
	// On the very first Peek, give `parser_override` extensions a chance to claim the whole
	// query. If one does, we yield its statements one at a time and skip the PEG path entirely.
	if (!override_resolved) {
		override_resolved = true;
		// Built-in Layer-1 connect-mode override: `CONNECT <name> EXECUTE <payload>` forms are claimed
		// here and turned into PassthroughStatements, so the PEG parser never sees the payload text.
		{
			vector<unique_ptr<SQLStatement>> connect_statements;
			if (TryParseConnectMode(options, sql, connect_statements)) {
				overridden_statements = make_uniq<vector<unique_ptr<SQLStatement>>>(std::move(connect_statements));
			}
		}
		if (!overridden_statements && options.extensions) {
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
	EnsureTokenized();
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
			current_statement = std::move(stmt);
			return true;
		}
		if (at_end_of_real_tokens()) {
			exhausted = true;
			return false;
		}
		// separator-only TLS in the middle of the input — loop and try the next.
	}
}

void ParseIterator::EnsureTokenized() {
	if (!tokens) {
		// Tokenize the full input once. Subsequent Peek/HasMore calls walk through `tokens` via
		// `token_cursor`; we never re-tokenize. Tokenization is grammar-free.
		tokens = make_uniq<vector<MatcherToken>>();
		ParserTokenizer tokenizer(sql, *tokens);
		tokenizer.TokenizeInput();
	}
}

bool ParseIterator::HasMore() {
	// A statement is already parsed and buffered by a prior Peek.
	if (current_statement) {
		return true;
	}
	if (exhausted) {
		return false;
	}
	// parser_override path: yield remaining overridden statements.
	if (overridden_statements) {
		return override_cursor < overridden_statements->size();
	}
	// PEG path: walk the token cursor without parsing. There is another statement iff a real token
	// (neither a `;` separator nor the end-of-input sentinel) remains ahead of the cursor.
	EnsureTokenized();
	for (idx_t i = token_cursor; i < tokens->size(); i++) {
		const auto type = (*tokens)[i].type;
		if (type == TokenType::END_OF_INPUT) {
			return false;
		}
		if (type != TokenType::TERMINATOR) {
			return true;
		}
	}
	return false;
}

unique_ptr<SQLStatement> ParseIterator::GetStatement() {
	if (!current_statement) {
		return nullptr;
	}
	return std::move(current_statement);
}

} // namespace duckdb
