/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  PGTimestamp and PGInterval typedefs and related macros.
 *
 * Note: this file must be includable in both frontend and backend contexts.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/datatype/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>

/*
 * PGTimestamp represents absolute time.
 *
 * PGInterval represents delta time. Keep track of months (and years), days,
 * and hours/minutes/seconds separately since the elapsed time spanned is
 * unknown until instantiated relative to an absolute time.
 *
 * Note that Postgres uses "time interval" to mean a bounded interval,
 * consisting of a beginning and ending time, not a time span - thomas 97/03/20
 *
 * Timestamps, as well as the h/m/s fields of intervals, are stored as
 * int64_t values with units of microseconds.  (Once upon a time they were
 * double values with units of seconds.)
 *
 * PGTimeOffset and pg_fsec_t are convenience typedefs for temporary variables.
 * Do not use pg_fsec_t in values stored on-disk.
 * Also, pg_fsec_t is only meant for *fractional* seconds; beware of overflow
 * if the value you need to store could be many seconds.
 */

typedef int64_t PGTimestamp;
typedef int64_t PGTimestampTz;
typedef int64_t PGTimeOffset;
typedef int32_t pg_fsec_t;			/* fractional seconds (in microseconds) */

typedef struct
{
	PGTimeOffset	time;			/* all time units other than days, months and
								 * years */
	int32_t		day;			/* days, after time for alignment */
	int32_t		month;			/* months and years, after time for alignment */
} PGInterval;
