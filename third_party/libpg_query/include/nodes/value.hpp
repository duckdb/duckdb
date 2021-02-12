/*-------------------------------------------------------------------------
 *
 * value.h
 *	  interface for PGValue nodes
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development PGGroup
 *
 * src/include/nodes/value.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "nodes/nodes.hpp"

namespace duckdb_libpgquery {

/*----------------------
 *		PGValue node
 *
 * The same PGValue struct is used for five node types: duckdb_libpgquery::T_PGInteger,
 * duckdb_libpgquery::T_PGFloat, duckdb_libpgquery::T_PGString, duckdb_libpgquery::T_PGBitString, T_Null.
 *
 * Integral values are actually represented by a machine integer,
 * but both floats and strings are represented as strings.
 * Using duckdb_libpgquery::T_PGFloat as the node type simply indicates that
 * the contents of the string look like a valid numeric literal.
 *
 * (Before Postgres 7.0, we used a double to represent duckdb_libpgquery::T_PGFloat,
 * but that creates loss-of-precision problems when the value is
 * ultimately destined to be converted to NUMERIC.  Since PGValue nodes
 * are only used in the parsing process, not for runtime data, it's
 * better to use the more general representation.)
 *
 * Note that an integer-looking string will get lexed as duckdb_libpgquery::T_PGFloat if
 * the value is too large to fit in a 'long'.
 *
 * Nulls, of course, don't need the value part at all.
 *----------------------
 */
typedef struct PGValue {
	PGNodeTag type; /* tag appropriately (eg. duckdb_libpgquery::T_PGString) */
	union ValUnion {
		long ival; /* machine integer */
		char *str; /* string */
	} val;
} PGValue;

#define intVal(v) (((PGValue *)(v))->val.ival)
#define floatVal(v) atof(((PGValue *)(v))->val.str)
#define strVal(v) (((PGValue *)(v))->val.str)

PGValue *makeInteger(long i);
PGValue *makeFloat(char *numericStr);
PGValue *makeString(const char *str);
PGValue *makeBitString(char *str);

}