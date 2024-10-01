#pragma once

#include <cstdint>
#include <string>

namespace duckdb_libpgquery {

enum class PGSimplifiedTokenType : uint8_t {
	PG_SIMPLIFIED_TOKEN_IDENTIFIER,
	PG_SIMPLIFIED_TOKEN_NUMERIC_CONSTANT,
	PG_SIMPLIFIED_TOKEN_STRING_CONSTANT,
	PG_SIMPLIFIED_TOKEN_OPERATOR,
	PG_SIMPLIFIED_TOKEN_KEYWORD,
	PG_SIMPLIFIED_TOKEN_COMMENT
};

struct PGSimplifiedToken {
	PGSimplifiedTokenType type;
	int32_t start;
};

enum class PGKeywordCategory : uint8_t {
	PG_KEYWORD_UNRESERVED = 0,
	PG_KEYWORD_COL_NAME = 1,
	PG_KEYWORD_TYPE_FUNC= 2,
	PG_KEYWORD_RESERVED = 3,
	PG_KEYWORD_NONE = 4
};


struct PGKeyword {
	std::string text;
	PGKeywordCategory category;
};

}
