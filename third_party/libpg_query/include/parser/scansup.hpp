/*-------------------------------------------------------------------------
 *
 * scansup.h
 *	  scanner support routines.  used by both the bootstrap lexer
 * as well as the normal lexer
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/scansup.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

extern char *scanstr(const char *s);

extern char *downcase_truncate_identifier(const char *ident, int len,
							 bool warn);

extern char *downcase_identifier(const char *ident, int len,
					bool warn, bool truncate);

extern void truncate_identifier(char *ident, int len, bool warn);

extern bool scanner_isspace(char ch);
