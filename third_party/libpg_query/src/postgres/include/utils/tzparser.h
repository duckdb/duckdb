/*-------------------------------------------------------------------------
 *
 * tzparser.h
 *	  Timezone offset file parsing definitions.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tzparser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TZPARSER_H
#define TZPARSER_H

#include "utils/datetime.h"

/*
 * The result of parsing a timezone configuration file is an array of
 * these structs, in order by abbrev.  We export this because datetime.c
 * needs it.
 */
typedef struct tzEntry
{
	/* the actual data */
	char	   *abbrev;			/* TZ abbreviation (downcased) */
	char	   *zone;			/* zone name if dynamic abbrev, else NULL */
	/* for a dynamic abbreviation, offset/is_dst are not used */
	int			offset;			/* offset in seconds from UTC */
	bool		is_dst;			/* true if a DST abbreviation */
	/* source information (for error messages) */
	int			lineno;
	const char *filename;
} tzEntry;


extern TimeZoneAbbrevTable *load_tzoffsets(const char *filename);

#endif   /* TZPARSER_H */
