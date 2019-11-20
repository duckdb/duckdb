/*-------------------------------------------------------------------------
 *
 * keywords.h
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/keywords.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3


typedef struct PGScanKeyword
{
	const char *name;			/* in lower case */
	int16_t		value;			/* grammar's token code */
	int16_t		category;		/* see codes above */
} PGScanKeyword;

extern const PGScanKeyword ScanKeywords[];
extern const int NumScanKeywords;

extern const PGScanKeyword *ScanKeywordLookup(const char *text,
				  const PGScanKeyword *keywords,
				  int num_keywords);
