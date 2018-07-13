/*-------------------------------------------------------------------------
 *
 * pg_ts_template.h
 *	definition of dictionary templates for tsearch
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_template.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TS_TEMPLATE_H
#define PG_TS_TEMPLATE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_ts_template definition.  cpp turns this into
 *		typedef struct FormData_pg_ts_template
 * ----------------
 */
#define TSTemplateRelationId	3764

CATALOG(pg_ts_template,3764)
{
	NameData	tmplname;		/* template name */
	Oid			tmplnamespace;	/* name space */
	regproc		tmplinit;		/* initialization method of dict (may be 0) */
	regproc		tmpllexize;		/* base method of dictionary */
} FormData_pg_ts_template;

typedef FormData_pg_ts_template *Form_pg_ts_template;

/* ----------------
 *		compiler constants for pg_ts_template
 * ----------------
 */
#define Natts_pg_ts_template				4
#define Anum_pg_ts_template_tmplname		1
#define Anum_pg_ts_template_tmplnamespace	2
#define Anum_pg_ts_template_tmplinit		3
#define Anum_pg_ts_template_tmpllexize		4

/* ----------------
 *		initial contents of pg_ts_template
 * ----------------
 */

DATA(insert OID = 3727 ( "simple" PGNSP dsimple_init dsimple_lexize ));
DESCR("simple dictionary: just lower case and check for stopword");
DATA(insert OID = 3730 ( "synonym" PGNSP dsynonym_init dsynonym_lexize ));
DESCR("synonym dictionary: replace word by its synonym");
DATA(insert OID = 3733 ( "ispell" PGNSP dispell_init dispell_lexize ));
DESCR("ispell dictionary");
DATA(insert OID = 3742 ( "thesaurus" PGNSP thesaurus_init thesaurus_lexize ));
DESCR("thesaurus dictionary: phrase by phrase substitution");

#endif   /* PG_TS_TEMPLATE_H */
