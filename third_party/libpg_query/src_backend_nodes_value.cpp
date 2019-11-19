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
 *	  implementation of Value nodes
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/value.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"

#include "nodes/parsenodes.hpp"

/*
 *	makeInteger
 */
Value *
makeInteger(long i)
{
	Value	   *v = makeNode(Value);

	v->type = T_Integer;
	v->val.ival = i;
	return v;
}

/*
 *	makeFloat
 *
 * Caller is responsible for passing a palloc'd string.
 */
Value *
makeFloat(char *numericStr)
{
	Value	   *v = makeNode(Value);

	v->type = T_Float;
	v->val.str = numericStr;
	return v;
}

/*
 *	makeString
 *
 * Caller is responsible for passing a palloc'd string.
 */
Value *
makeString(const char *str)
{
	Value	   *v = makeNode(Value);

	v->type = T_String;
	v->val.str = (char*) str;
	return v;
}

/*
 *	makeBitString
 *
 * Caller is responsible for passing a palloc'd string.
 */

