/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - GetDatabaseEncoding
 * - DatabaseEncoding
 * - pg_get_client_encoding
 * - ClientEncoding
 * - pg_mbcliplen
 * - pg_encoding_mbcliplen
 * - cliplen
 * - pg_mblen
 * - pg_mbstrlen_with_len
 * - SetDatabaseEncoding
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * mbutils.c
 *	  This file contains functions for encoding conversion.
 *
 * The string-conversion functions in this file share some API quirks.
 * Note the following:
 *
 * The functions return a palloc'd, null-terminated string if conversion
 * is required.  However, if no conversion is performed, the given source
 * string pointer is returned as-is.
 *
 * Although the presence of a length argument means that callers can pass
 * non-null-terminated strings, care is required because the same string
 * will be passed back if no conversion occurs.  Such callers *must* check
 * whether result == src and handle that case differently.
 *
 * If the source and destination encodings are the same, the source string
 * is returned without any verification; it's assumed to be valid data.
 * If that might not be the case, the caller is responsible for validating
 * the string using a separate call to pg_verify_mbstr().  Whenever the
 * source and destination encodings are different, the functions ensure that
 * the result is validly encoded according to the destination encoding.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/mbutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * When converting strings between different encodings, we assume that space
 * for converted result is 4-to-1 growth in the worst case. The rate for
 * currently supported encoding pairs are within 3 (SJIS JIS X0201 half width
 * kanna -> UTF8 is the worst case).  So "4" should be enough for the moment.
 *
 * Note that this is not the same as the maximum character width in any
 * particular encoding.
 */
#define MAX_CONVERSION_GROWTH  4

/*
 * We maintain a simple linked list caching the fmgr lookup info for the
 * currently selected conversion functions, as well as any that have been
 * selected previously in the current session.  (We remember previous
 * settings because we must be able to restore a previous setting during
 * transaction rollback, without doing any fresh catalog accesses.)
 *
 * Since we'll never release this data, we just keep it in TopMemoryContext.
 */
typedef struct ConvProcInfo
{
	int			s_encoding;		/* server and client encoding IDs */
	int			c_encoding;
	FmgrInfo	to_server_info; /* lookup info for conversion procs */
	FmgrInfo	to_client_info;
} ConvProcInfo;

	/* List of ConvProcInfo */

/*
 * These variables point to the currently active conversion functions,
 * or are NULL when no conversion is needed.
 */



/*
 * These variables track the currently-selected encodings.
 */
static const pg_enc2name *ClientEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];
static const pg_enc2name *DatabaseEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];


/*
 * During backend startup we can't set client encoding because we (a)
 * can't look up the conversion functions, and (b) may not know the database
 * encoding yet either.  So SetClientEncoding() just accepts anything and
 * remembers it for InitializeClientEncoding() to apply later.
 */




/* Internal functions */
static char *perform_default_encoding_conversion(const char *src,
									int len, bool is_client_to_server);
static int	cliplen(const char *str, int len, int limit);


/*
 * Prepare for a future call to SetClientEncoding.  Success should mean
 * that SetClientEncoding is guaranteed to succeed for this encoding request.
 *
 * (But note that success before backend_startup_complete does not guarantee
 * success after ...)
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */


/*
 * Set the active client encoding and set up the conversion-function pointers.
 * PrepareClientEncoding should have been called previously for this encoding.
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */


/*
 * Initialize client encoding conversions.
 *		Called from InitPostgres() once during backend startup.
 */


/*
 * returns the current client encoding
 */
int
pg_get_client_encoding(void)
{
	return ClientEncoding->encoding;
}

/*
 * returns the current client encoding name
 */


/*
 * Convert src string to another encoding (general case).
 *
 * See the notes about string conversion functions at the top of this file.
 */


/*
 * Convert string to encoding encoding_name. The source
 * encoding is the DB encoding.
 *
 * BYTEA convert_to(TEXT string, NAME encoding_name) */


/*
 * Convert string from encoding encoding_name. The destination
 * encoding is the DB encoding.
 *
 * TEXT convert_from(BYTEA string, NAME encoding_name) */


/*
 * Convert string between two arbitrary encodings.
 *
 * BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
 */


/*
 * get the length of the string considered as text in the specified
 * encoding. Raises an error if the data is not valid in that
 * encoding.
 *
 * INT4 length (BYTEA string, NAME src_encoding_name)
 */


/*
 * Get maximum multibyte character length in the specified encoding.
 *
 * Note encoding is specified numerically, not by name as above.
 */


/*
 * Convert client encoding to server encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */


/*
 * Convert any encoding to server encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 *
 * Unlike the other string conversion functions, this will apply validation
 * even if encoding == DatabaseEncoding->encoding.  This is because this is
 * used to process data coming in from outside the database, and we never
 * want to just assume validity.
 */


/*
 * Convert server encoding to client encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */


/*
 * Convert server encoding to any encoding.
 *
 * See the notes about string conversion functions at the top of this file.
 */


/*
 *	Perform default encoding conversion using cached FmgrInfo. Since
 *	this function does not access database at all, it is safe to call
 *	outside transactions.  If the conversion has not been set up by
 *	SetClientEncoding(), no conversion is performed.
 */



/* convert a multibyte string to a wchar */


/* convert a multibyte string to a wchar with a limited length */


/* same, with any encoding */


/* convert a wchar string to a multibyte */


/* convert a wchar string to a multibyte with a limited length */


/* same, with any encoding */


/* returns the byte length of a multibyte character */
int
pg_mblen(const char *mbstr)
{
	return ((*pg_wchar_table[DatabaseEncoding->encoding].mblen) ((const unsigned char *) mbstr));
}

/* returns the display length of a multibyte character */


/* returns the length (counted in wchars) of a multibyte string */


/* returns the length (counted in wchars) of a multibyte string
 * (not necessarily NULL terminated)
 */
int
pg_mbstrlen_with_len(const char *mbstr, int limit)
{
	int			len = 0;

	/* optimization for single byte encoding */
	if (pg_database_encoding_max_length() == 1)
		return limit;

	while (limit > 0 && *mbstr)
	{
		int			l = pg_mblen(mbstr);

		limit -= l;
		mbstr += l;
		len++;
	}
	return len;
}

/*
 * returns the byte length of a multibyte string
 * (not necessarily NULL terminated)
 * that is no longer than limit.
 * this function does not break multibyte character boundary.
 */
int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	return pg_encoding_mbcliplen(DatabaseEncoding->encoding, mbstr,
								 len, limit);
}

/*
 * pg_mbcliplen with specified encoding
 */
int
pg_encoding_mbcliplen(int encoding, const char *mbstr,
					  int len, int limit)
{
	mblen_converter mblen_fn;
	int			clen = 0;
	int			l;

	/* optimization for single byte encoding */
	if (pg_encoding_max_length(encoding) == 1)
		return cliplen(mbstr, len, limit);

	mblen_fn = pg_wchar_table[encoding].mblen;

	while (len > 0 && *mbstr)
	{
		l = (*mblen_fn) ((const unsigned char *) mbstr);
		if ((clen + l) > limit)
			break;
		clen += l;
		if (clen == limit)
			break;
		len -= l;
		mbstr += l;
	}
	return clen;
}

/*
 * Similar to pg_mbcliplen except the limit parameter specifies the
 * character length, not the byte length.
 */


/* mbcliplen for any single-byte encoding */
static int
cliplen(const char *str, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && str[l])
		l++;
	return l;
}

void
SetDatabaseEncoding(int encoding)
{
	if (!PG_VALID_BE_ENCODING(encoding))
		elog(ERROR, "invalid database encoding: %d", encoding);

	DatabaseEncoding = &pg_enc2name_tbl[encoding];
	Assert(DatabaseEncoding->encoding == encoding);
}



#ifdef ENABLE_NLS
/*
 * Make one bind_textdomain_codeset() call, translating a pg_enc to a gettext
 * codeset.  Fails for MULE_INTERNAL, an encoding unknown to gettext; can also
 * fail for gettext-internal causes like out-of-memory.
 */
static bool
raw_pg_bind_textdomain_codeset(const char *domainname, int encoding)
{
	bool		elog_ok = (CurrentMemoryContext != NULL);
	int			i;

	for (i = 0; pg_enc2gettext_tbl[i].name != NULL; i++)
	{
		if (pg_enc2gettext_tbl[i].encoding == encoding)
		{
			if (bind_textdomain_codeset(domainname,
										pg_enc2gettext_tbl[i].name) != NULL)
				return true;

			if (elog_ok)
				elog(LOG, "bind_textdomain_codeset failed");
			else
				write_stderr("bind_textdomain_codeset failed");

			break;
		}
	}

	return false;
}

/*
 * Bind a gettext message domain to the codeset corresponding to the database
 * encoding.  For SQL_ASCII, instead bind to the codeset implied by LC_CTYPE.
 * Return the MessageEncoding implied by the new settings.
 *
 * On most platforms, gettext defaults to the codeset implied by LC_CTYPE.
 * When that matches the database encoding, we don't need to do anything.  In
 * CREATE DATABASE, we enforce or trust that the locale's codeset matches the
 * database encoding, except for the C locale.  (On Windows, we also permit a
 * discrepancy under the UTF8 encoding.)  For the C locale, explicitly bind
 * gettext to the right codeset.
 *
 * On Windows, gettext defaults to the Windows ANSI code page.  This is a
 * convenient departure for software that passes the strings to Windows ANSI
 * APIs, but we don't do that.  Compel gettext to use database encoding or,
 * failing that, the LC_CTYPE encoding as it would on other platforms.
 *
 * This function is called before elog() and palloc() are usable.
 */
int
pg_bind_textdomain_codeset(const char *domainname)
{
	bool		elog_ok = (CurrentMemoryContext != NULL);
	int			encoding = GetDatabaseEncoding();
	int			new_msgenc;

#ifndef WIN32
	const char *ctype = setlocale(LC_CTYPE, NULL);

	if (pg_strcasecmp(ctype, "C") == 0 || pg_strcasecmp(ctype, "POSIX") == 0)
#endif
		if (encoding != PG_SQL_ASCII &&
			raw_pg_bind_textdomain_codeset(domainname, encoding))
			return encoding;

	new_msgenc = pg_get_encoding_from_locale(NULL, elog_ok);
	if (new_msgenc < 0)
		new_msgenc = PG_SQL_ASCII;

#ifdef WIN32
	if (!raw_pg_bind_textdomain_codeset(domainname, new_msgenc))
		/* On failure, the old message encoding remains valid. */
		return GetMessageEncoding();
#endif

	return new_msgenc;
}
#endif

/*
 * The database encoding, also called the server encoding, represents the
 * encoding of data stored in text-like data types.  Affected types include
 * cstring, text, varchar, name, xml, and json.
 */
int
GetDatabaseEncoding(void)
{
	return DatabaseEncoding->encoding;
}







/*
 * gettext() returns messages in this encoding.  This often matches the
 * database encoding, but it differs for SQL_ASCII databases, for processes
 * not attached to a database, and under a database encoding lacking iconv
 * support (MULE_INTERNAL).
 */


#ifdef WIN32
/*
 * Result is palloc'ed null-terminated utf16 string. The character length
 * is also passed to utf16len if not null. Returns NULL iff failed.
 */
WCHAR *
pgwin32_message_to_UTF16(const char *str, int len, int *utf16len)
{
	WCHAR	   *utf16;
	int			dstlen;
	UINT		codepage;

	codepage = pg_enc2name_tbl[GetMessageEncoding()].codepage;

	/*
	 * Use MultiByteToWideChar directly if there is a corresponding codepage,
	 * or double conversion through UTF8 if not.  Double conversion is needed,
	 * for example, in an ENCODING=LATIN8, LC_CTYPE=C database.
	 */
	if (codepage != 0)
	{
		utf16 = (WCHAR *) palloc(sizeof(WCHAR) * (len + 1));
		dstlen = MultiByteToWideChar(codepage, 0, str, len, utf16, len);
		utf16[dstlen] = (WCHAR) 0;
	}
	else
	{
		char	   *utf8;

		/*
		 * XXX pg_do_encoding_conversion() requires a transaction.  In the
		 * absence of one, hope for the input to be valid UTF8.
		 */
		if (IsTransactionState())
		{
			utf8 = (char *) pg_do_encoding_conversion((unsigned char *) str,
													  len,
													  GetMessageEncoding(),
													  PG_UTF8);
			if (utf8 != str)
				len = strlen(utf8);
		}
		else
			utf8 = (char *) str;

		utf16 = (WCHAR *) palloc(sizeof(WCHAR) * (len + 1));
		dstlen = MultiByteToWideChar(CP_UTF8, 0, utf8, len, utf16, len);
		utf16[dstlen] = (WCHAR) 0;

		if (utf8 != str)
			pfree(utf8);
	}

	if (dstlen == 0 && len > 0)
	{
		pfree(utf16);
		return NULL;			/* error */
	}

	if (utf16len)
		*utf16len = dstlen;
	return utf16;
}

#endif
