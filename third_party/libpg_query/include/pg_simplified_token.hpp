#pragma once

#include <cstdint>

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

}
