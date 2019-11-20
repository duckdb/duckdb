/*-------------------------------------------------------------------------
 *
 * pg_wchar.h
 *	  multibyte-character support
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/mb/pg_wchar.h
 *
 *	NOTES
 *		This is used both by the backend and by libpq, but should not be
 *		included by libpq client programs.  In particular, a libpq client
 *		should not assume that the encoding IDs used by the version of libpq
 *		it's linked to match up with the IDs declared here.
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>

/*
 * The pg_wchar type
 */
typedef unsigned int pg_wchar;
