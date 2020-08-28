/*-------------------------------------------------------------------------
 *
 * pgtime.h
 *	  PostgreSQL internal timezone library
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 *
 * IDENTIFICATION
 *	  src/include/pgtime.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>

/*
 * The API of this library is generally similar to the corresponding
 * C library functions, except that we use pg_time_t which (we hope) is
 * 64 bits wide, and which is most definitely signed not unsigned.
 */

typedef int64_t pg_time_t;

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* origin 1, not 0! */
	int			tm_year;		/* relative to 1900 */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};

typedef struct pg_tz pg_tz;
typedef struct pg_tzenum pg_tzenum;

/* Maximum length of a timezone name (not including trailing null) */
#define TZ_STRLEN_MAX 255

/* these functions are in localtime.c */

struct pg_tm *pg_localtime(const pg_time_t *timep, const pg_tz *tz);
struct pg_tm *pg_gmtime(const pg_time_t *timep);
int pg_next_dst_boundary(const pg_time_t *timep,
					 long int *before_gmtoff,
					 int *before_isdst,
					 pg_time_t *boundary,
					 long int *after_gmtoff,
					 int *after_isdst,
					 const pg_tz *tz);
bool pg_interpret_timezone_abbrev(const char *abbrev,
							 const pg_time_t *timep,
							 long int *gmtoff,
							 int *isdst,
							 const pg_tz *tz);
bool pg_get_timezone_offset(const pg_tz *tz, long int *gmtoff);
const char *pg_get_timezone_name(pg_tz *tz);
bool pg_tz_acceptable(pg_tz *tz);

/* these functions and variables are in pgtz.c */

pg_tz *session_timezone;
pg_tz *log_timezone;

void pg_timezone_initialize(void);
pg_tz *pg_tzset(const char *tzname);
pg_tz *pg_tzset_offset(long gmtoffset);

pg_tzenum *pg_tzenumerate_start(void);
pg_tz *pg_tzenumerate_next(pg_tzenum *dir);
void pg_tzenumerate_end(pg_tzenum *dir);
