/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  Definitions for the SQL "timestamp" and "interval" types.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIMESTAMP_H
#define TIMESTAMP_H

#include "datatype/timestamp.h"
#include "fmgr.h"
#include "pgtime.h"


/*
 * Macros for fmgr-callable functions.
 *
 * For Timestamp, we make use of the same support routines as for int64
 * or float8.  Therefore Timestamp is pass-by-reference if and only if
 * int64 or float8 is!
 */
#ifdef HAVE_INT64_TIMESTAMP

#define DatumGetTimestamp(X)  ((Timestamp) DatumGetInt64(X))
#define DatumGetTimestampTz(X)	((TimestampTz) DatumGetInt64(X))
#define DatumGetIntervalP(X)  ((Interval *) DatumGetPointer(X))

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

#define DatumGetTimestamp(X)  ((Timestamp) DatumGetFloat8(X))
#define DatumGetTimestampTz(X)	((TimestampTz) DatumGetFloat8(X))
#define DatumGetIntervalP(X)  ((Interval *) DatumGetPointer(X))

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
#define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) * (int64) 1000))
#else
#define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) / 1000.0))
#endif


/* Set at postmaster start */
extern TimestampTz PgStartTime;

/* Set at configuration reload */
extern TimestampTz PgReloadTime;


/*
 * timestamp.c prototypes
 */

extern Datum timestamp_in(PG_FUNCTION_ARGS);
extern Datum timestamp_out(PG_FUNCTION_ARGS);
extern Datum timestamp_recv(PG_FUNCTION_ARGS);
extern Datum timestamp_send(PG_FUNCTION_ARGS);
extern Datum timestamptypmodin(PG_FUNCTION_ARGS);
extern Datum timestamptypmodout(PG_FUNCTION_ARGS);
extern Datum timestamp_transform(PG_FUNCTION_ARGS);
extern Datum timestamp_scale(PG_FUNCTION_ARGS);
extern Datum timestamp_eq(PG_FUNCTION_ARGS);
extern Datum timestamp_ne(PG_FUNCTION_ARGS);
extern Datum timestamp_lt(PG_FUNCTION_ARGS);
extern Datum timestamp_le(PG_FUNCTION_ARGS);
extern Datum timestamp_ge(PG_FUNCTION_ARGS);
extern Datum timestamp_gt(PG_FUNCTION_ARGS);
extern Datum timestamp_finite(PG_FUNCTION_ARGS);
extern Datum timestamp_cmp(PG_FUNCTION_ARGS);
extern Datum timestamp_sortsupport(PG_FUNCTION_ARGS);
extern Datum timestamp_hash(PG_FUNCTION_ARGS);
extern Datum timestamp_smaller(PG_FUNCTION_ARGS);
extern Datum timestamp_larger(PG_FUNCTION_ARGS);

extern Datum timestamp_eq_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_ne_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_lt_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_le_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_gt_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_ge_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_cmp_timestamptz(PG_FUNCTION_ARGS);

extern Datum make_timestamp(PG_FUNCTION_ARGS);
extern Datum make_timestamptz(PG_FUNCTION_ARGS);
extern Datum make_timestamptz_at_timezone(PG_FUNCTION_ARGS);

extern Datum timestamptz_eq_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_ne_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_lt_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_le_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_gt_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_ge_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_cmp_timestamp(PG_FUNCTION_ARGS);

extern Datum interval_in(PG_FUNCTION_ARGS);
extern Datum interval_out(PG_FUNCTION_ARGS);
extern Datum interval_recv(PG_FUNCTION_ARGS);
extern Datum interval_send(PG_FUNCTION_ARGS);
extern Datum intervaltypmodin(PG_FUNCTION_ARGS);
extern Datum intervaltypmodout(PG_FUNCTION_ARGS);
extern Datum interval_transform(PG_FUNCTION_ARGS);
extern Datum interval_scale(PG_FUNCTION_ARGS);
extern Datum interval_eq(PG_FUNCTION_ARGS);
extern Datum interval_ne(PG_FUNCTION_ARGS);
extern Datum interval_lt(PG_FUNCTION_ARGS);
extern Datum interval_le(PG_FUNCTION_ARGS);
extern Datum interval_ge(PG_FUNCTION_ARGS);
extern Datum interval_gt(PG_FUNCTION_ARGS);
extern Datum interval_finite(PG_FUNCTION_ARGS);
extern Datum interval_cmp(PG_FUNCTION_ARGS);
extern Datum interval_hash(PG_FUNCTION_ARGS);
extern Datum interval_smaller(PG_FUNCTION_ARGS);
extern Datum interval_larger(PG_FUNCTION_ARGS);
extern Datum interval_justify_interval(PG_FUNCTION_ARGS);
extern Datum interval_justify_hours(PG_FUNCTION_ARGS);
extern Datum interval_justify_days(PG_FUNCTION_ARGS);
extern Datum make_interval(PG_FUNCTION_ARGS);

extern Datum timestamp_trunc(PG_FUNCTION_ARGS);
extern Datum interval_trunc(PG_FUNCTION_ARGS);
extern Datum timestamp_part(PG_FUNCTION_ARGS);
extern Datum interval_part(PG_FUNCTION_ARGS);
extern Datum timestamp_zone_transform(PG_FUNCTION_ARGS);
extern Datum timestamp_zone(PG_FUNCTION_ARGS);
extern Datum timestamp_izone_transform(PG_FUNCTION_ARGS);
extern Datum timestamp_izone(PG_FUNCTION_ARGS);
extern Datum timestamp_timestamptz(PG_FUNCTION_ARGS);

extern Datum timestamptz_in(PG_FUNCTION_ARGS);
extern Datum timestamptz_out(PG_FUNCTION_ARGS);
extern Datum timestamptz_recv(PG_FUNCTION_ARGS);
extern Datum timestamptz_send(PG_FUNCTION_ARGS);
extern Datum timestamptztypmodin(PG_FUNCTION_ARGS);
extern Datum timestamptztypmodout(PG_FUNCTION_ARGS);
extern Datum timestamptz_scale(PG_FUNCTION_ARGS);
extern Datum timestamptz_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamptz_zone(PG_FUNCTION_ARGS);
extern Datum timestamptz_izone(PG_FUNCTION_ARGS);
extern Datum timestamptz_timestamptz(PG_FUNCTION_ARGS);

extern Datum interval_um(PG_FUNCTION_ARGS);
extern Datum interval_pl(PG_FUNCTION_ARGS);
extern Datum interval_mi(PG_FUNCTION_ARGS);
extern Datum interval_mul(PG_FUNCTION_ARGS);
extern Datum mul_d_interval(PG_FUNCTION_ARGS);
extern Datum interval_div(PG_FUNCTION_ARGS);
extern Datum interval_accum(PG_FUNCTION_ARGS);
extern Datum interval_accum_inv(PG_FUNCTION_ARGS);
extern Datum interval_avg(PG_FUNCTION_ARGS);

extern Datum timestamp_mi(PG_FUNCTION_ARGS);
extern Datum timestamp_pl_interval(PG_FUNCTION_ARGS);
extern Datum timestamp_mi_interval(PG_FUNCTION_ARGS);
extern Datum timestamp_age(PG_FUNCTION_ARGS);
extern Datum overlaps_timestamp(PG_FUNCTION_ARGS);

extern Datum timestamptz_pl_interval(PG_FUNCTION_ARGS);
extern Datum timestamptz_mi_interval(PG_FUNCTION_ARGS);
extern Datum timestamptz_age(PG_FUNCTION_ARGS);
extern Datum timestamptz_trunc(PG_FUNCTION_ARGS);
extern Datum timestamptz_part(PG_FUNCTION_ARGS);

extern Datum now(PG_FUNCTION_ARGS);
extern Datum statement_timestamp(PG_FUNCTION_ARGS);
extern Datum clock_timestamp(PG_FUNCTION_ARGS);

extern Datum pg_postmaster_start_time(PG_FUNCTION_ARGS);
extern Datum pg_conf_load_time(PG_FUNCTION_ARGS);

extern Datum generate_series_timestamp(PG_FUNCTION_ARGS);
extern Datum generate_series_timestamptz(PG_FUNCTION_ARGS);

/* Internal routines (not fmgr-callable) */

extern TimestampTz GetCurrentTimestamp(void);
extern void TimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					long *secs, int *microsecs);
extern bool TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec);

/*
 * Prototypes for functions to deal with integer timestamps, when the native
 * format is float timestamps.
 */
#ifndef HAVE_INT64_TIMESTAMP
extern int64 GetCurrentIntegerTimestamp(void);
extern TimestampTz IntegerTimestampToTimestampTz(int64 timestamp);
#else
#define GetCurrentIntegerTimestamp()	GetCurrentTimestamp()
#define IntegerTimestampToTimestampTz(timestamp) (timestamp)
#endif

extern TimestampTz time_t_to_timestamptz(pg_time_t tm);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);

extern const char *timestamptz_to_str(TimestampTz t);

extern int	tm2timestamp(struct pg_tm * tm, fsec_t fsec, int *tzp, Timestamp *dt);
extern int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm * tm,
			 fsec_t *fsec, const char **tzn, pg_tz *attimezone);
extern void dt2time(Timestamp dt, int *hour, int *min, int *sec, fsec_t *fsec);

extern int	interval2tm(Interval span, struct pg_tm * tm, fsec_t *fsec);
extern int	tm2interval(struct pg_tm * tm, fsec_t fsec, Interval *span);

extern Timestamp SetEpochTimestamp(void);
extern void GetEpochTime(struct pg_tm * tm);

extern int	timestamp_cmp_internal(Timestamp dt1, Timestamp dt2);

/* timestamp comparison works for timestamptz also */
#define timestamptz_cmp_internal(dt1,dt2)	timestamp_cmp_internal(dt1, dt2)

extern int	isoweek2j(int year, int week);
extern void isoweek2date(int woy, int *year, int *mon, int *mday);
extern void isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday);
extern int	date2isoweek(int year, int mon, int mday);
extern int	date2isoyear(int year, int mon, int mday);
extern int	date2isoyearday(int year, int mon, int mday);

#endif   /* TIMESTAMP_H */
