/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - ScanKeywords
 * - NumScanKeywords
 * - ScanKeywordLookup
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * keywords.c
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/keywords.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"
#include <string.h>
#include <string>
#include <memory>

#include "parser/gramparse.hpp"

#include "parser/kwlist.hpp"

namespace duckdb_libpgquery {

/*
 * ScanKeywordLookup - see if a given word is a keyword
 *
 * The table to be searched is passed explicitly, so that this can be used
 * to search keyword lists other than the standard list appearing above.
 *
 * Returns a pointer to the PGScanKeyword table entry, or NULL if no match.
 *
 * The match is done case-insensitively.  Note that we deliberately use a
 * dumbed-down case conversion that will only translate 'A'-'Z' into 'a'-'z',
 * even if we are in a locale where tolower() would produce more or different
 * translations.  This is to conform to the SQL99 spec, which says that
 * keywords are to be matched in this way even though non-keyword identifiers
 * receive a different case-normalization mapping.
 */
const PGScanKeyword *ScanKeywordLookup(const char *text, const PGScanKeyword *keywords, int num_keywords) {
	int len, i;
	const PGScanKeyword *low;
	const PGScanKeyword *high;

	len = strlen(text);
	auto data = std::unique_ptr<char[]>(new char[len + 1]);
	auto word = data.get();
	/* We assume all keywords are shorter than NAMEDATALEN. */

	/*
	 * Apply an ASCII-only downcasing.  We must not use tolower() since it may
	 * produce the wrong translation in some locales (eg, Turkish).
	 */
	for (i = 0; i < len; i++) {
		char ch = text[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		word[i] = ch;
	}
	word[len] = '\0';

	/*
	 * Now do a binary search using plain strcmp() comparison.
	 */
	low = keywords;
	high = keywords + (num_keywords - 1);
	while (low <= high) {
		const PGScanKeyword *middle;
		int difference;

		middle = low + (high - low) / 2;
		difference = strcmp(middle->name, word);
		if (difference == 0)
			return middle;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}

	return NULL;
}
}