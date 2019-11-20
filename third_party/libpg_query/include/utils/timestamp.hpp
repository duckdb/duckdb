/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  Definitions for the SQL "timestamp" and "interval" types.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "datatype/timestamp.hpp"
#include "fmgr.hpp"
#include "pgtime.hpp"


/*
 * Macros for fmgr-callable functions.
 *
 * For PGTimestamp, we make use of the same support routines as for int64_t
 * or float8.  Therefore PGTimestamp is pass-by-reference if and only if
 * int64_t or float8 is!
 */
#ifdef HAVE_INT64_TIMESTAMP

#define DatumGetTimestamp(X)  ((PGTimestamp) DatumGetInt64(X))
#define DatumGetTimestampTz(X)	((PGTimestampTz) DatumGetInt64(X))
#define DatumGetIntervalP(X)  ((PGInterval *) DatumGetPointer(X))

#define TimestampGetDatum(X) Int64GetDatum(X)
#define TimestampTzGetDatum(X) Int64GetDatum(X)
#define IntervalPGetDatum(X) PointerGetDatum(X)

#define PG_GETARG_TIMESTAMP(n) DatumGetTimestamp(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMESTAMPTZ(n) DatumGetTimestampTz(PG_GETARG_DATUM(n))
#define PG_GETARG_INTERVAL_P(n) DatumGetIntervalP(PG_GETARG_DATUM(n))

#define PG_RETURN_TIMESTAMP(x) return TimestampGetDatum(x)
#define PG_RETURN_TIMESTAMPTZ(x) return TimestampTzGetDatum(x)
#define PG_RETURN_INTERVAL_P(x) return IntervalPGetDatum(x)
#else							/* !HAVE_INT64_TIMESTAMP */

#define DatumGetTimestamp(X)  ((PGTimestamp) DatumGetFloat8(X))
#define DatumGetTimestampTz(X)	((PGTimestampTz) DatumGetFloat8(X))
#define DatumGetIntervalP(X)  ((PGInterval *) DatumGetPointer(X))

#define TimestampGetDatum(X) Float8GetDatum(X)
#define TimestampTzGetDatum(X) Float8GetDatum(X)
#define IntervalPGetDatum(X) PointerGetDatum(X)

#define PG_GETARG_TIMESTAMP(n) DatumGetTimestamp(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMESTAMPTZ(n) DatumGetTimestampTz(PG_GETARG_DATUM(n))
#define PG_GETARG_INTERVAL_P(n) DatumGetIntervalP(PG_GETARG_DATUM(n))

#define PG_RETURN_TIMESTAMP(x) return TimestampGetDatum(x)
#define PG_RETURN_TIMESTAMPTZ(x) return TimestampTzGetDatum(x)
#define PG_RETURN_INTERVAL_P(x) return IntervalPGetDatum(x)
#endif   /* HAVE_INT64_TIMESTAMP */


#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_FULL_RANGE (0x7FFF)
#define INTERVAL_RANGE_MASK (0x7FFF)
#define INTERVAL_FULL_PRECISION (0xFFFF)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_TYPMOD(p,r) ((((r) & INTERVAL_RANGE_MASK) << 16) | ((p) & INTERVAL_PRECISION_MASK))
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)

#ifdef HAVE_INT64_TIMESTAMP
#define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) * (int64_t) 1000))
#else
#define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) / 1000.0))
#endif


/* Set at postmaster start */
extern PGTimestampTz PgStartTime;

/* Set at configuration reload */
extern PGTimestampTz PgReloadTime;


/*
 * timestamp.c prototypes
 */

extern PGDatum timestamp_in(PG_FUNCTION_ARGS);
extern PGDatum timestamp_out(PG_FUNCTION_ARGS);
extern PGDatum timestamp_recv(PG_FUNCTION_ARGS);
extern PGDatum timestamp_send(PG_FUNCTION_ARGS);
extern PGDatum timestamptypmodin(PG_FUNCTION_ARGS);
extern PGDatum timestamptypmodout(PG_FUNCTION_ARGS);
extern PGDatum timestamp_transform(PG_FUNCTION_ARGS);
extern PGDatum timestamp_scale(PG_FUNCTION_ARGS);
extern PGDatum timestamp_eq(PG_FUNCTION_ARGS);
extern PGDatum timestamp_ne(PG_FUNCTION_ARGS);
extern PGDatum timestamp_lt(PG_FUNCTION_ARGS);
extern PGDatum timestamp_le(PG_FUNCTION_ARGS);
extern PGDatum timestamp_ge(PG_FUNCTION_ARGS);
extern PGDatum timestamp_gt(PG_FUNCTION_ARGS);
extern PGDatum timestamp_finite(PG_FUNCTION_ARGS);
extern PGDatum timestamp_cmp(PG_FUNCTION_ARGS);
extern PGDatum timestamp_sortsupport(PG_FUNCTION_ARGS);
extern PGDatum timestamp_hash(PG_FUNCTION_ARGS);
extern PGDatum timestamp_smaller(PG_FUNCTION_ARGS);
extern PGDatum timestamp_larger(PG_FUNCTION_ARGS);

extern PGDatum timestamp_eq_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_ne_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_lt_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_le_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_gt_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_ge_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum timestamp_cmp_timestamptz(PG_FUNCTION_ARGS);

extern PGDatum make_timestamp(PG_FUNCTION_ARGS);
extern PGDatum make_timestamptz(PG_FUNCTION_ARGS);
extern PGDatum make_timestamptz_at_timezone(PG_FUNCTION_ARGS);

extern PGDatum timestamptz_eq_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_ne_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_lt_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_le_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_gt_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_ge_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_cmp_timestamp(PG_FUNCTION_ARGS);

extern PGDatum interval_in(PG_FUNCTION_ARGS);
extern PGDatum interval_out(PG_FUNCTION_ARGS);
extern PGDatum interval_recv(PG_FUNCTION_ARGS);
extern PGDatum interval_send(PG_FUNCTION_ARGS);
extern PGDatum intervaltypmodin(PG_FUNCTION_ARGS);
extern PGDatum intervaltypmodout(PG_FUNCTION_ARGS);
extern PGDatum interval_transform(PG_FUNCTION_ARGS);
extern PGDatum interval_scale(PG_FUNCTION_ARGS);
extern PGDatum interval_eq(PG_FUNCTION_ARGS);
extern PGDatum interval_ne(PG_FUNCTION_ARGS);
extern PGDatum interval_lt(PG_FUNCTION_ARGS);
extern PGDatum interval_le(PG_FUNCTION_ARGS);
extern PGDatum interval_ge(PG_FUNCTION_ARGS);
extern PGDatum interval_gt(PG_FUNCTION_ARGS);
extern PGDatum interval_finite(PG_FUNCTION_ARGS);
extern PGDatum interval_cmp(PG_FUNCTION_ARGS);
extern PGDatum interval_hash(PG_FUNCTION_ARGS);
extern PGDatum interval_smaller(PG_FUNCTION_ARGS);
extern PGDatum interval_larger(PG_FUNCTION_ARGS);
extern PGDatum interval_justify_interval(PG_FUNCTION_ARGS);
extern PGDatum interval_justify_hours(PG_FUNCTION_ARGS);
extern PGDatum interval_justify_days(PG_FUNCTION_ARGS);
extern PGDatum make_interval(PG_FUNCTION_ARGS);

extern PGDatum timestamp_trunc(PG_FUNCTION_ARGS);
extern PGDatum interval_trunc(PG_FUNCTION_ARGS);
extern PGDatum timestamp_part(PG_FUNCTION_ARGS);
extern PGDatum interval_part(PG_FUNCTION_ARGS);
extern PGDatum timestamp_zone_transform(PG_FUNCTION_ARGS);
extern PGDatum timestamp_zone(PG_FUNCTION_ARGS);
extern PGDatum timestamp_izone_transform(PG_FUNCTION_ARGS);
extern PGDatum timestamp_izone(PG_FUNCTION_ARGS);
extern PGDatum timestamp_timestamptz(PG_FUNCTION_ARGS);

extern PGDatum timestamptz_in(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_out(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_recv(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_send(PG_FUNCTION_ARGS);
extern PGDatum timestamptztypmodin(PG_FUNCTION_ARGS);
extern PGDatum timestamptztypmodout(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_scale(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_timestamp(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_zone(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_izone(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_timestamptz(PG_FUNCTION_ARGS);

extern PGDatum interval_um(PG_FUNCTION_ARGS);
extern PGDatum interval_pl(PG_FUNCTION_ARGS);
extern PGDatum interval_mi(PG_FUNCTION_ARGS);
extern PGDatum interval_mul(PG_FUNCTION_ARGS);
extern PGDatum mul_d_interval(PG_FUNCTION_ARGS);
extern PGDatum interval_div(PG_FUNCTION_ARGS);
extern PGDatum interval_accum(PG_FUNCTION_ARGS);
extern PGDatum interval_accum_inv(PG_FUNCTION_ARGS);
extern PGDatum interval_avg(PG_FUNCTION_ARGS);

extern PGDatum timestamp_mi(PG_FUNCTION_ARGS);
extern PGDatum timestamp_pl_interval(PG_FUNCTION_ARGS);
extern PGDatum timestamp_mi_interval(PG_FUNCTION_ARGS);
extern PGDatum timestamp_age(PG_FUNCTION_ARGS);
extern PGDatum overlaps_timestamp(PG_FUNCTION_ARGS);

extern PGDatum timestamptz_pl_interval(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_mi_interval(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_age(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_trunc(PG_FUNCTION_ARGS);
extern PGDatum timestamptz_part(PG_FUNCTION_ARGS);

extern PGDatum now(PG_FUNCTION_ARGS);
extern PGDatum statement_timestamp(PG_FUNCTION_ARGS);
extern PGDatum clock_timestamp(PG_FUNCTION_ARGS);

extern PGDatum pg_postmaster_start_time(PG_FUNCTION_ARGS);
extern PGDatum pg_conf_load_time(PG_FUNCTION_ARGS);

extern PGDatum generate_series_timestamp(PG_FUNCTION_ARGS);
extern PGDatum generate_series_timestamptz(PG_FUNCTION_ARGS);

/* Internal routines (not fmgr-callable) */

extern PGTimestampTz GetCurrentTimestamp(void);
extern void TimestampDifference(PGTimestampTz start_time, PGTimestampTz stop_time,
					long *secs, int *microsecs);
extern bool TimestampDifferenceExceeds(PGTimestampTz start_time,
						   PGTimestampTz stop_time,
						   int msec);

/*
 * Prototypes for functions to deal with integer timestamps, when the native
 * format is float timestamps.
 */
#ifndef HAVE_INT64_TIMESTAMP
extern int64_t GetCurrentIntegerTimestamp(void);
extern PGTimestampTz IntegerTimestampToTimestampTz(int64_t timestamp);
#else
#define GetCurrentIntegerTimestamp()	GetCurrentTimestamp()
#define IntegerTimestampToTimestampTz(timestamp) (timestamp)
#endif

extern PGTimestampTz time_t_to_timestamptz(pg_time_t tm);
extern pg_time_t timestamptz_to_time_t(PGTimestampTz t);

extern const char *timestamptz_to_str(PGTimestampTz t);

extern int	tm2timestamp(struct pg_tm * tm, pg_fsec_t fsec, int *tzp, PGTimestamp *dt);
extern int timestamp2tm(PGTimestamp dt, int *tzp, struct pg_tm * tm,
			 pg_fsec_t *fsec, const char **tzn, pg_tz *attimezone);
extern void dt2time(PGTimestamp dt, int *hour, int *min, int *sec, pg_fsec_t *fsec);

extern int	interval2tm(PGInterval span, struct pg_tm * tm, pg_fsec_t *fsec);
extern int	tm2interval(struct pg_tm * tm, pg_fsec_t fsec, PGInterval *span);

extern PGTimestamp SetEpochTimestamp(void);
extern void GetEpochTime(struct pg_tm * tm);

extern int	timestamp_cmp_internal(PGTimestamp dt1, PGTimestamp dt2);

/* timestamp comparison works for timestamptz also */
#define timestamptz_cmp_internal(dt1,dt2)	timestamp_cmp_internal(dt1, dt2)

extern int	isoweek2j(int year, int week);
extern void isoweek2date(int woy, int *year, int *mon, int *mday);
extern void isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday);
extern int	date2isoweek(int year, int mon, int mday);
extern int	date2isoyear(int year, int mon, int mday);
extern int	date2isoyearday(int year, int mon, int mday);
