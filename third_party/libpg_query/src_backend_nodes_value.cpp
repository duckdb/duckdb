/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - makeInteger
 * - makeString
 * - makeFloat
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * value.c
 *	  implementation of PGValue nodes
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development PGGroup
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/value.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"

#include "nodes/parsenodes.hpp"
#include <string>
#include <cstring>

namespace duckdb_libpgquery {

/*
 *	makeInteger
 */
PGValue *makeInteger(long i) {
	PGValue *v = makeNode(PGValue);

	v->type = T_PGInteger;
	v->val.ival = i;
	return v;
}

/*
 *	makeFloat
 *
 * Caller is responsible for passing a palloc'd string.
 */
PGValue *makeFloat(char *numericStr) {
	PGValue *v = makeNode(PGValue);

	v->type = T_PGFloat;
	v->val.str = numericStr;
	return v;
}

/*
 *	makeString
 *
 * Caller is responsible for passing a palloc'd string.
 */
PGValue *makeString(const char *str) {
	PGValue *v = makeNode(PGValue);

	v->type = T_PGString;
	v->val.str = (char *)str;
	return v;
}

/*
 *	makeBitString
 *
 * Caller is responsible for passing a palloc'd string.
 */

}
