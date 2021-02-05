#include "stripped_sqlite_int.h"
#include <ctype.h>

#define sqlite3Toupper(x) toupper((unsigned char)(x))
#define sqlite3Tolower(x) tolower((unsigned char)(x))

/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
static const unsigned char sqlite3Utf8Trans1[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};

u32 sqlite3Utf8Read(const unsigned char **pz /* Pointer to string from which to read char */
) {
	unsigned int c;

	/* Same as READ_UTF8() above but without the zTerm parameter.
	** For this routine, we assume the UTF8 string is always zero-terminated.
	*/
	c = *((*pz)++);
	if (c >= 0xc0) {
		c = sqlite3Utf8Trans1[c - 0xc0];
		while ((*(*pz) & 0xc0) == 0x80) {
			c = (c << 6) + (0x3f & *((*pz)++));
		}
		if (c < 0x80 || (c & 0xFFFFF800) == 0xD800 || (c & 0xFFFFFFFE) == 0xFFFE) {
			c = 0xFFFD;
		}
	}
	return c;
}

/*
** A structure defining how to do GLOB-style comparisons.
*/
struct compareInfo {
	u8 matchAll; /* "*" or "%" */
	u8 matchOne; /* "?" or "_" */
	u8 matchSet; /* "[" or 0 */
	u8 noCase;   /* true to ignore case differences */
};

/*
** For LIKE and GLOB matching on EBCDIC machines, assume that every
** character is exactly one byte in size.  Also, provde the Utf8Read()
** macro for fast reading of the next character in the common case where
** the next character is ASCII.
*/
#define Utf8Read(A) (A[0] < 0x80 ? *(A++) : sqlite3Utf8Read(&A))

static const struct compareInfo globInfo = {'*', '?', '[', 0};
/* The correct SQL-92 behavior is for the LIKE operator to ignore
** case.  Thus  'a' LIKE 'A' would be true. */
static const struct compareInfo likeInfoNorm = {'%', '_', 0, 1};
/* If SQLITE_CASE_SENSITIVE_LIKE is defined, then the LIKE operator
** is case sensitive causing 'a' LIKE 'A' to be false */
// static const struct compareInfo likeInfoAlt = { '%', '_',   0, 0 };

/*
** Possible error returns from patternMatch()
*/
#define SQLITE_MATCH           0
#define SQLITE_NOMATCH         1
#define SQLITE_NOWILDCARDMATCH 2

/*
** Compare two UTF-8 strings for equality where the first string is
** a GLOB or LIKE expression.  Return values:
**
**    SQLITE_MATCH:            Match
**    SQLITE_NOMATCH:          No match
**    SQLITE_NOWILDCARDMATCH:  No match in spite of having * or % wildcards.
**
** Globbing rules:
**
**      '*'       Matches any sequence of zero or more characters.
**
**      '?'       Matches exactly one character.
**
**     [...]      Matches one character from the enclosed list of
**                characters.
**
**     [^...]     Matches one character not in the enclosed list.
**
** With the [...] and [^...] matching, a ']' character can be included
** in the list by making it the first character after '[' or '^'.  A
** range of characters can be specified using '-'.  Example:
** "[a-z]" matches any single lower-case letter.  To match a '-', make
** it the last character in the list.
**
** Like matching rules:
**
**      '%'       Matches any sequence of zero or more characters
**
***     '_'       Matches any one character
**
**      Ec        Where E is the "esc" character and c is any other
**                character, including '%', '_', and esc, match exactly c.
**
** The comments within this routine usually assume glob matching.
**
** This routine is usually quick, but can be N**2 in the worst case.
*/
static int patternCompare(const u8 *zPattern,              /* The glob pattern */
                          const u8 *zString,               /* The string to compare against the glob */
                          const struct compareInfo *pInfo, /* Information about how to do the compare */
                          uint32_t matchOther              /* The escape char (LIKE) or '[' (GLOB) */
) {
	uint32_t c, c2;                      /* Next pattern and input string chars */
	uint32_t matchOne = pInfo->matchOne; /* "?" or "_" */
	uint32_t matchAll = pInfo->matchAll; /* "*" or "%" */
	u8 noCase = pInfo->noCase;           /* True if uppercase==lowercase */
	const u8 *zEscaped = 0;              /* One past the last escaped input char */

	while ((c = Utf8Read(zPattern)) != 0) {
		if (c == matchAll) { /* Match "*" */
			/* Skip over multiple "*" characters in the pattern.  If there
			** are also "?" characters, skip those as well, but consume a
			** single character of the input string for each "?" skipped */
			while ((c = Utf8Read(zPattern)) == matchAll || c == matchOne) {
				if (c == matchOne && sqlite3Utf8Read(&zString) == 0) {
					return SQLITE_NOWILDCARDMATCH;
				}
			}
			if (c == 0) {
				return SQLITE_MATCH; /* "*" at the end of the pattern matches */
			} else if (c == matchOther) {
				if (pInfo->matchSet == 0) {
					c = sqlite3Utf8Read(&zPattern);
					if (c == 0)
						return SQLITE_NOWILDCARDMATCH;
				} else {
					/* "[...]" immediately follows the "*".  We have to do a slow
					** recursive search in this case, but it is an unusual case. */
					assert(matchOther < 0x80); /* '[' is a single-byte character */
					while (*zString) {
						int bMatch = patternCompare(&zPattern[-1], zString, pInfo, matchOther);
						if (bMatch != SQLITE_NOMATCH)
							return bMatch;
						SQLITE_SKIP_UTF8(zString);
					}
					return SQLITE_NOWILDCARDMATCH;
				}
			}

			/* At this point variable c contains the first character of the
			** pattern string past the "*".  Search in the input string for the
			** first matching character and recursively continue the match from
			** that point.
			**
			** For a case-insensitive search, set variable cx to be the same as
			** c but in the other case and search the input string for either
			** c or cx.
			*/
			if (c <= 0x80) {
				char zStop[3];
				int bMatch;
				if (noCase) {
					zStop[0] = sqlite3Toupper(c);
					zStop[1] = sqlite3Tolower(c);
					zStop[2] = 0;
				} else {
					zStop[0] = c;
					zStop[1] = 0;
				}
				while (1) {
					zString += strcspn((const char *)zString, zStop);
					if (zString[0] == 0)
						break;
					zString++;
					bMatch = patternCompare(zPattern, zString, pInfo, matchOther);
					if (bMatch != SQLITE_NOMATCH)
						return bMatch;
				}
			} else {
				int bMatch;
				while ((c2 = Utf8Read(zString)) != 0) {
					if (c2 != c)
						continue;
					bMatch = patternCompare(zPattern, zString, pInfo, matchOther);
					if (bMatch != SQLITE_NOMATCH)
						return bMatch;
				}
			}
			return SQLITE_NOWILDCARDMATCH;
		}
		if (c == matchOther) {
			if (pInfo->matchSet == 0) {
				c = sqlite3Utf8Read(&zPattern);
				if (c == 0)
					return SQLITE_NOMATCH;
				zEscaped = zPattern;
			} else {
				uint32_t prior_c = 0;
				int seen = 0;
				int invert = 0;
				c = sqlite3Utf8Read(&zString);
				if (c == 0)
					return SQLITE_NOMATCH;
				c2 = sqlite3Utf8Read(&zPattern);
				if (c2 == '^') {
					invert = 1;
					c2 = sqlite3Utf8Read(&zPattern);
				}
				if (c2 == ']') {
					if (c == ']')
						seen = 1;
					c2 = sqlite3Utf8Read(&zPattern);
				}
				while (c2 && c2 != ']') {
					if (c2 == '-' && zPattern[0] != ']' && zPattern[0] != 0 && prior_c > 0) {
						c2 = sqlite3Utf8Read(&zPattern);
						if (c >= prior_c && c <= c2)
							seen = 1;
						prior_c = 0;
					} else {
						if (c == c2) {
							seen = 1;
						}
						prior_c = c2;
					}
					c2 = sqlite3Utf8Read(&zPattern);
				}
				if (c2 == 0 || (seen ^ invert) == 0) {
					return SQLITE_NOMATCH;
				}
				continue;
			}
		}
		c2 = Utf8Read(zString);
		if (c == c2)
			continue;
		if (noCase && sqlite3Tolower(c) == sqlite3Tolower(c2) && c < 0x80 && c2 < 0x80) {
			continue;
		}
		if (c == matchOne && zPattern != zEscaped && c2 != 0)
			continue;
		return SQLITE_NOMATCH;
	}
	return *zString == 0 ? SQLITE_MATCH : SQLITE_NOMATCH;
}

/*
** The sqlite3_strglob() interface.  Return 0 on a match (like strcmp()) and
** non-zero if there is no match.
*/
int sqlite3_strglob(const char *zGlobPattern, const char *zString) {
	return patternCompare((u8 *)zGlobPattern, (u8 *)zString, &globInfo, '[');
}

/*
** The sqlite3_strlike() interface.  Return 0 on a match and non-zero for
** a miss - like strcmp().
*/
int sqlite3_strlike(const char *zPattern, const char *zStr, unsigned int esc) {
	return patternCompare((u8 *)zPattern, (u8 *)zStr, &likeInfoNorm, esc);
}
