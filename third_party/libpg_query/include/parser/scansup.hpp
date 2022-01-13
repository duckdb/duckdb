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

namespace duckdb_libpgquery {

char *scanstr(const char *s);

char *downcase_truncate_identifier(const char *ident, int len, bool warn);

char *downcase_identifier(const char *ident, int len, bool warn, bool truncate);

bool scanner_isspace(char ch);

void set_preserve_identifier_case(bool downcase);
bool get_preserve_identifier_case();

}