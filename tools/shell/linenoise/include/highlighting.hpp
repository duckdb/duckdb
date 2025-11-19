//===----------------------------------------------------------------------===//
//                         DuckDB
//
// highlighting.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct searchMatch;

enum class tokenType : uint8_t {
	TOKEN_IDENTIFIER,
	TOKEN_NUMERIC_CONSTANT,
	TOKEN_STRING_CONSTANT,
	TOKEN_OPERATOR,
	TOKEN_KEYWORD,
	TOKEN_COMMENT,
	TOKEN_CONTINUATION,
	TOKEN_CONTINUATION_SELECTED,
	TOKEN_BRACKET,
	TOKEN_ERROR
};

enum class HighlightingType { KEYWORD, CONSTANT, COMMENT, ERROR_TOKEN, CONTINUATION, CONTINUATION_SELECTED };
enum class ExtraHighlightingType { NONE, UNDERLINE, BOLD };

struct highlightToken {
	tokenType type;
	size_t start = 0;
	ExtraHighlightingType extra_highlighting = ExtraHighlightingType::NONE;
};

struct ExtraHighlighting {
	ExtraHighlighting() : start(0), end(0), type(ExtraHighlightingType::NONE) {
	}

	idx_t start;
	idx_t end;
	ExtraHighlightingType type;
};

class Highlighting {
public:
	static void Enable();
	static void Disable();
	static bool IsEnabled();
	static void SetHighlightingColor(HighlightingType type, const char *color);

	static vector<highlightToken> Tokenize(char *buf, size_t len, bool is_dot_command);
	static void AddExtraHighlighting(size_t len, vector<highlightToken> &tokens, ExtraHighlighting extra_highlighting);
	static string HighlightText(char *buf, size_t len, size_t start_pos, size_t end_pos,
	                            const vector<highlightToken> &tokens);
};

} // namespace duckdb
