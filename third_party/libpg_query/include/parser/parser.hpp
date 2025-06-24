/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "nodes/parsenodes.hpp"
#include "pg_simplified_token.hpp"
#include <vector>
namespace duckdb_libpgquery {

typedef enum PGBackslashQuoteType {
	PG_BACKSLASH_QUOTE_OFF,
	PG_BACKSLASH_QUOTE_ON,
	PG_BACKSLASH_QUOTE_SAFE_ENCODING
} PGBackslashQuoteType;

/* Primary entry point for the raw parsing functions */
PGList *raw_parser(const char *str);

PGKeywordCategory is_keyword(const char *str);
std::vector<PGKeyword> keyword_list();

std::vector<PGSimplifiedToken> tokenize(const char *str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
PGList *SystemFuncName(const char *name);
PGTypeName *SystemTypeName(const char *name);

}
