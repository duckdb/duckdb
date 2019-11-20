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


typedef enum PGBackslashQuoteType
{
	PG_BACKSLASH_QUOTE_OFF,
	PG_BACKSLASH_QUOTE_ON,
	PG_BACKSLASH_QUOTE_SAFE_ENCODING
} PGBackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern __thread  int backslash_quote;
extern __thread  bool escape_string_warning;
extern __thread  bool standard_conforming_strings;

/* Primary entry point for the raw parsing functions */
extern PGList *raw_parser(const char *str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern PGList *SystemFuncName(const char *name);
extern PGTypeName *SystemTypeName(const char *name);

