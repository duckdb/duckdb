/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - ScanKeywords
 * - NumScanKeywords
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * keywords.c
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/keywords.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "parser/gramparse.h"

#define PG_KEYWORD(a,b,c) {a,b,c},


const ScanKeyword ScanKeywords[] = {
#include "parser/kwlist.h"
};

const int	NumScanKeywords = lengthof(ScanKeywords);
