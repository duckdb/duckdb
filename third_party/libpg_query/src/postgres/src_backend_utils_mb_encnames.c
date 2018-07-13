/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - pg_enc2name_tbl
 *--------------------------------------------------------------------
 */

/*
 * Encoding names and routines for work with it. All
 * in this file is shared between FE and BE.
 *
 * src/backend/utils/mb/encnames.c
 */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#include "utils/builtins.h"
#endif

#include <ctype.h>
#include <unistd.h>

#include "mb/pg_wchar.h"


/* ----------
 * All encoding names, sorted:		 *** A L P H A B E T I C ***
 *
 * All names must be without irrelevant chars, search routines use
 * isalnum() chars only. It means ISO-8859-1, iso_8859-1 and Iso8859_1
 * are always converted to 'iso88591'. All must be lower case.
 *
 * The table doesn't contain 'cs' aliases (like csISOLatin1). It's needed?
 *
 * Karel Zak, Aug 2001
 * ----------
 */
typedef struct pg_encname
{
	const char *name;
	pg_enc		encoding;
} pg_encname;



/* ----------
 * These are "official" encoding names.
 * XXX must be sorted by the same order as enum pg_enc (in mb/pg_wchar.h)
 * ----------
 */
#ifndef WIN32
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name }
#else
#define DEF_ENC2NAME(name, codepage) { #name, PG_##name, codepage }
#endif
const pg_enc2name pg_enc2name_tbl[] =
{
	DEF_ENC2NAME(SQL_ASCII, 0),
	DEF_ENC2NAME(EUC_JP, 20932),
	DEF_ENC2NAME(EUC_CN, 20936),
	DEF_ENC2NAME(EUC_KR, 51949),
	DEF_ENC2NAME(EUC_TW, 0),
	DEF_ENC2NAME(EUC_JIS_2004, 20932),
	DEF_ENC2NAME(UTF8, 65001),
	DEF_ENC2NAME(MULE_INTERNAL, 0),
	DEF_ENC2NAME(LATIN1, 28591),
	DEF_ENC2NAME(LATIN2, 28592),
	DEF_ENC2NAME(LATIN3, 28593),
	DEF_ENC2NAME(LATIN4, 28594),
	DEF_ENC2NAME(LATIN5, 28599),
	DEF_ENC2NAME(LATIN6, 0),
	DEF_ENC2NAME(LATIN7, 0),
	DEF_ENC2NAME(LATIN8, 0),
	DEF_ENC2NAME(LATIN9, 28605),
	DEF_ENC2NAME(LATIN10, 0),
	DEF_ENC2NAME(WIN1256, 1256),
	DEF_ENC2NAME(WIN1258, 1258),
	DEF_ENC2NAME(WIN866, 866),
	DEF_ENC2NAME(WIN874, 874),
	DEF_ENC2NAME(KOI8R, 20866),
	DEF_ENC2NAME(WIN1251, 1251),
	DEF_ENC2NAME(WIN1252, 1252),
	DEF_ENC2NAME(ISO_8859_5, 28595),
	DEF_ENC2NAME(ISO_8859_6, 28596),
	DEF_ENC2NAME(ISO_8859_7, 28597),
	DEF_ENC2NAME(ISO_8859_8, 28598),
	DEF_ENC2NAME(WIN1250, 1250),
	DEF_ENC2NAME(WIN1253, 1253),
	DEF_ENC2NAME(WIN1254, 1254),
	DEF_ENC2NAME(WIN1255, 1255),
	DEF_ENC2NAME(WIN1257, 1257),
	DEF_ENC2NAME(KOI8U, 21866),
	DEF_ENC2NAME(SJIS, 932),
	DEF_ENC2NAME(BIG5, 950),
	DEF_ENC2NAME(GBK, 936),
	DEF_ENC2NAME(UHC, 949),
	DEF_ENC2NAME(GB18030, 54936),
	DEF_ENC2NAME(JOHAB, 0),
	DEF_ENC2NAME(SHIFT_JIS_2004, 932)
};

/* ----------
 * These are encoding names for gettext.
 *
 * This covers all encodings except MULE_INTERNAL, which is alien to gettext.
 * ----------
 */



/* ----------
 * Encoding checks, for error returns -1 else encoding id
 * ----------
 */






/* ----------
 * Remove irrelevant chars from encoding name
 * ----------
 */


/* ----------
 * Search encoding by encoding name
 *
 * Returns encoding ID, or -1 for error
 * ----------
 */
#ifdef FRONTEND
#else
#endif

#ifndef FRONTEND

#endif



#ifndef FRONTEND


#endif
