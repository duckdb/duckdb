#include "duckdb/parser/connect_mode/connect_mode_parser.hpp"

#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"

namespace duckdb {

//! Connect-mode (Layer 1) grammar — see connect_mode.gram for the canonical declaration.
//! Duplicated here as a string literal so the parser is self-contained and doesn't depend on the
//! main grammar's inlining pipeline. Keep in sync with connect_mode.gram if you edit either.
static constexpr const char *CONNECT_MODE_GRAMMAR = R"(
Program <- ';'* Body?
Body <- Chunk (';'+ Chunk)* ';'*
Chunk <- ExecuteChunk / ForbiddenConnectExecute / ForbiddenConnect1 / ConnectChunk / ForbiddenConnect2 / ForbiddenConnect3 / ForbiddenDisconnect / DisconnectChunk / RawChunk
ConnectChunk <- 'CONNECT' (Identifier / 'LOCAL' / StringLiteral)
DisconnectChunk <- 'DISCONNECT'
ExecuteChunk <- 'CONNECT' Identifier 'EXECUTE' Raw
ForbiddenConnectExecute <- 'CONNECT' 'EXECUTE' Raw
ForbiddenConnect1 <- 'CONNECT' (Identifier / 'LOCAL' / StringLiteral) Raw
ForbiddenConnect2 <- 'CONNECT' Raw
ForbiddenConnect3 <- 'CONNECT'
ForbiddenDisconnect <- 'DISCONNECT' Raw
RawChunk <- Raw
Raw <- InStatementToken+
)";
// Identifier and StringLiteral are provided as built-in overrides by
// MatcherFactory::CreateMatcherNoOverrides — no need to define them in the grammar.

//! Lazily-compiled, shared across all ConnectModeParser instances. Grammar compilation isn't
//! free (parses the grammar string into a matcher tree), so we cache it for the process lifetime.
static shared_ptr<PEGMatcher> GetConnectModeMatcher() {
	static shared_ptr<PEGMatcher> cached = PEGMatcher::CompileGrammar(CONNECT_MODE_GRAMMAR, "Program");
	return cached;
}

ConnectModeParser::ConnectModeParser(const string &sql_p)
    : sql(sql_p), matcher(GetConnectModeMatcher()), result_allocator(make_uniq<ParseResultAllocator>()) {
	// Use ParserTokenizer (not BaseTokenizer) so unquoted `;` is emitted as a TERMINATOR token —
	// the grammar relies on it to separate statements.
	ParserTokenizer tokenizer(sql, tokens);
	tokenizer.TokenizeInput();
	if (tokens.empty()) {
		return;
	}
	vector<MatcherSuggestion> suggestions;
	idx_t max_token_index = 0;
	MatchState state(tokens, suggestions, *result_allocator, max_token_index, /* preserve_identifier_case */ true);
	auto matched = matcher->ProgramMatcher().MatchParseResult(state);
	if (!matched || state.token_index < state.tokens.size()) {
		// Empty `tokens` is handled above. Reaching here means there were real tokens that the
		// Layer 1 grammar couldn't classify — a clear drift signal. Surface it rather than letting
		// the caller silently see zero chunks.
		throw ParserException("ConnectModeParser failed to parse input: \"%s\"", sql);
	}
	program = matched->Cast<ListParseResult>();
}

ConnectModeParser::~ConnectModeParser() = default;

//! Walk a parse subtree and record the min and max char-offset of any leaf node with a set offset.
//! Leaf nodes (IDENTIFIER, KEYWORD, STRING, NUMBER, OPERATOR) carry the char offset of their token;
//! container nodes (LIST/REPEAT/OPTIONAL/CHOICE) recurse into their children.
static void CollectLeafOffsets(ParseResult &node, optional_idx &min_off, optional_idx &max_off) {
	switch (node.type) {
	case ParseResultType::LIST:
		for (auto &c : node.Cast<ListParseResult>().GetChildren()) {
			CollectLeafOffsets(c.get(), min_off, max_off);
		}
		return;
	case ParseResultType::REPEAT:
		for (auto &c : node.Cast<RepeatParseResult>().GetChildren()) {
			CollectLeafOffsets(c.get(), min_off, max_off);
		}
		return;
	case ParseResultType::OPTIONAL: {
		auto &opt = node.Cast<OptionalParseResult>();
		if (opt.HasResult()) {
			CollectLeafOffsets(opt.GetResult(), min_off, max_off);
		}
		return;
	}
	case ParseResultType::CHOICE:
		CollectLeafOffsets(node.Cast<ChoiceParseResult>().GetResult(), min_off, max_off);
		return;
	default:
		break;
	}
	// Leaf — record its char offset if set.
	if (!node.offset.IsValid()) {
		return;
	}
	idx_t off = node.offset.GetIndex();
	if (!min_off.IsValid() || off < min_off.GetIndex()) {
		min_off = optional_idx(off);
	}
	if (!max_off.IsValid() || off > max_off.GetIndex()) {
		max_off = optional_idx(off);
	}
}

//! Slice the original SQL string for the source range covered by `node`. Returns empty string if
//! the subtree contains no offset-bearing leaves. The end of the range is computed by looking up
//! the last leaf's token in `tokens` and adding its length.
//!
//! When `include_trailing_semicolon` is true, the range is extended past any intermediate whitespace
//! to include a trailing `;` if present. The chunk-level slice uses this so that `chunk.text` is a
//! faithful round-trip of the user-typed statement (e.g. so `QueryLog` records `SELECT 1;` instead
//! of `SELECT 1`). Sub-slices like the EXECUTE payload do NOT include the trailing `;` — there is
//! none at that level, since the `;` is consumed by `Program`'s top-level `(';' Chunk)*`.
static string SliceSubtreeText(const string &sql, const vector<MatcherToken> &tokens, ParseResult &node,
                               bool include_trailing_semicolon = false) {
	optional_idx min_off;
	optional_idx max_off;
	CollectLeafOffsets(node, min_off, max_off);
	if (!min_off.IsValid() || !max_off.IsValid()) {
		return string();
	}
	// Compute end_char from the START of the next token in source order, not from the last leaf's
	// (text-derived) length. Some tokens are *rewritten* during tokenization — most notably,
	// `$tag$content$tag$` is stored with text `'content'`, whose length doesn't match the source
	// span. Using `last_token.offset + last_token.length` would truncate the chunk mid-string for
	// those cases. The boundary up to (but not including) the next token's offset is always a safe
	// upper bound for the last leaf's source span.
	idx_t end_char = sql.size();
	for (idx_t i = 0; i < tokens.size(); i++) {
		if (tokens[i].offset > max_off.GetIndex()) {
			end_char = tokens[i].offset;
			break;
		}
	}
	if (include_trailing_semicolon) {
		idx_t probe = end_char;
		while (probe < sql.size() && StringUtil::CharacterIsSpace(sql[probe])) {
			probe++;
		}
		if (probe < sql.size() && sql[probe] == ';') {
			end_char = probe + 1;
		}
	}
	idx_t start_char = min_off.GetIndex();
	return sql.substr(start_char, end_char - start_char);
}

//! Classify a Chunk LIST by inspecting the underlying CHOICE-resolved rule name.
//! The Chunk node has shape: LIST (Chunk) → CHOICE → LIST (ConnectChunk / DisconnectChunk / ...)
void ConnectModeParser::ClassifyChunk(ParseResult &chunk_node, ConnectModeChunk &out) {
	auto &chunk_list = chunk_node.Cast<ListParseResult>();
	auto &inner = chunk_list.GetChild(0);
	// The CHOICE wrapper exposes its resolved alternative via GetResult().
	auto &resolved = inner.Cast<ChoiceParseResult>().GetResult();
	const auto &name = resolved.name;
	if (name == "ConnectChunk" || name == "DisconnectChunk") {
		out.type = ConnectModeChunk::Type::CONTROL;
	} else if (name == "ExecuteChunk") {
		out.type = ConnectModeChunk::Type::EXECUTE;
		// ExecuteChunk LIST shape: [KEYWORD CONNECT, IDENTIFIER target, KEYWORD EXECUTE, LIST(Raw)]
		auto &exec_list = resolved.Cast<ListParseResult>();
		auto &target_node = exec_list.GetChild(1);
		auto &payload_node = exec_list.GetChild(3);
		out.target = target_node.Cast<IdentifierParseResult>().identifier.GetIdentifierName();
		out.payload = SliceSubtreeText(sql, tokens, payload_node);
	} else if (name == "ForbiddenConnect1") {
		out.type = ConnectModeChunk::Type::FORBIDDEN;
		out.forbidden_reason = ConnectModeChunk::ForbiddenReason::EXTRA_TOKENS_AFTER_CONNECT_TARGET;
	} else if (name == "ForbiddenConnect2") {
		out.type = ConnectModeChunk::Type::FORBIDDEN;
		out.forbidden_reason = ConnectModeChunk::ForbiddenReason::INVALID_CONNECT_TARGET;
	} else if (name == "ForbiddenConnect3") {
		out.type = ConnectModeChunk::Type::FORBIDDEN;
		out.forbidden_reason = ConnectModeChunk::ForbiddenReason::MISSING_CONNECT_TARGET;
	} else if (name == "ForbiddenConnectExecute") {
		out.type = ConnectModeChunk::Type::FORBIDDEN;
		out.forbidden_reason = ConnectModeChunk::ForbiddenReason::CONNECT_EXECUTE_MISSING_TARGET;
	} else if (name == "ForbiddenDisconnect") {
		out.type = ConnectModeChunk::Type::FORBIDDEN;
		out.forbidden_reason = ConnectModeChunk::ForbiddenReason::EXTRA_TOKENS_AFTER_DISCONNECT;
	} else {
		// "RawChunk" or anything else: treat as raw.
		out.type = ConnectModeChunk::Type::RAW;
	}
	out.text = SliceSubtreeText(sql, tokens, chunk_node, /* include_trailing_semicolon */ true);
}

bool ConnectModeParser::TryGetNext(ConnectModeChunk &out_chunk) {
	if (!program) {
		return false;
	}
	// Program shape: LIST [';'*-OPTIONAL, Body-OPTIONAL].
	//   - child[0] is the leading `';'*` (Optional(Repeat(';'))). We skip it — leading semicolons
	//     are not chunks.
	//   - child[1] is the optional Body. When present (input was non-empty after leading `;`s),
	//     Body is a LIST [Chunk, OPTIONAL(REPEAT([';' Chunk])), OPTIONAL(';')] — same shape the
	//     walker used to look at directly under Program before the leading-`;` change.
	auto &body_optional = program->GetChild(1);
	if (body_optional.type != ParseResultType::OPTIONAL) {
		return false;
	}
	auto &body_opt = body_optional.Cast<OptionalParseResult>();
	if (!body_opt.HasResult()) {
		return false;
	}
	auto &body = body_opt.GetResult().Cast<ListParseResult>();

	if (cursor == 0) {
		auto &first_chunk = body.GetChild(0);
		ClassifyChunk(first_chunk, out_chunk);
		cursor++;
		return true;
	}
	// PEG `*` compiles as Optional(Repeat(...)), so child[1] is an OPTIONAL wrapping a REPEAT.
	auto &optional_node = body.GetChild(1);
	if (optional_node.type != ParseResultType::OPTIONAL) {
		return false;
	}
	auto &optional = optional_node.Cast<OptionalParseResult>();
	if (!optional.HasResult()) {
		return false;
	}
	auto &repeat_node = optional.GetResult();
	if (repeat_node.type != ParseResultType::REPEAT) {
		return false;
	}
	auto &repeat = repeat_node.Cast<RepeatParseResult>();
	auto children = repeat.GetChildren();
	idx_t repeat_idx = cursor - 1;
	if (repeat_idx >= children.size()) {
		return false;
	}
	// Each repeat iteration is a LIST [KEYWORD ";", LIST(Chunk)] — we want the second child.
	auto &pair = children[repeat_idx].get().Cast<ListParseResult>();
	auto &chunk_node = pair.GetChild(1);
	ClassifyChunk(chunk_node, out_chunk);
	cursor++;
	return true;
}

vector<ConnectModeChunk> ConnectModeParser::AllRemaining() {
	vector<ConnectModeChunk> result;
	ConnectModeChunk chunk;
	while (TryGetNext(chunk)) {
		result.push_back(std::move(chunk));
	}
	return result;
}

} // namespace duckdb
