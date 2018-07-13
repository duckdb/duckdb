/*-------------------------------------------------------------------------
 *
 * keywords.h
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/keywords.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KEYWORDS_H
#define KEYWORDS_H

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3


typedef struct ScanKeyword
{
	const char *name;			/* in lower case */
	int16		value;			/* grammar's token code */
	int16		category;		/* see codes above */
} ScanKeyword;

extern PGDLLIMPORT const ScanKeyword ScanKeywords[];
extern PGDLLIMPORT const int NumScanKeywords;

extern const ScanKeyword *ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords);

#endif   /* KEYWORDS_H */
