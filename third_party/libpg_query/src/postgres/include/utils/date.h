/*-------------------------------------------------------------------------
 *
 * date.h
 *	  Definitions for the SQL "date" and "time" types.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/date.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATE_H
#define DATE_H

#include <math.h>

#include "fmgr.h"


typedef int32 DateADT;

#ifdef HAVE_INT64_TIMESTAMP
typedef int64 TimeADT;
#else
typedef float8 TimeADT;
#endif

typedef struct
{
	TimeADT		time;			/* all time units other than months and years */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT;

/*
 * Infinity and minus infinity must be the max and min values of DateADT.
 */
#define DATEVAL_NOBEGIN		((DateADT) PG_INT32_MIN)
#define DATEVAL_NOEND		((DateADT) PG_INT32_MAX)

#define DATE_NOBEGIN(j)		((j) = DATEVAL_NOBEGIN)
#define DATE_IS_NOBEGIN(j)	((j) == DATEVAL_NOBEGIN)
#define DATE_NOEND(j)		((j) = DATEVAL_NOEND)
#define DATE_IS_NOEND(j)	((j) == DATEVAL_NOEND)
#define DATE_NOT_FINITE(j)	(DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

/*
 * Macros for fmgr-callable functions.
 *
 * For TimeADT, we make use of the same support routines as for float8 or int64.
 * Therefore TimeADT is pass-by-reference if and only if float8 or int64 is!
 */
#ifdef HAVE_INT64_TIMESTAMP

#define MAX_TIME_PRECISION 6

#define DatumGetDateADT(X)	  ((DateADT) DatumGetInt32(X))
#define DatumGetTimeADT(X)	  ((TimeADT) DatumGetInt64(X))
#define DatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

#define DateADTGetDatum(X)	  Int32GetDatum(X)
#define TimeADTGetDatum(X)	  Int64GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)
#else							/* !HAVE_INT64_TIMESTAMP */

#define MAX_TIME_PRECISION 10

/* round off to MAX_TIME_PRECISION decimal places */
#define TIME_PREC_INV 10000000000.0
#define TIMEROUND(j) (rint(((double) (j)) * TIME_PREC_INV) / TIME_PREC_INV)

#define DatumGetDateADT(X)	  ((DateADT) DatumGetInt32(X))
#define DatumGetTimeADT(X)	  ((TimeADT) DatumGetFloat8(X))
#define DatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

#define DateADTGetDatum(X)	  Int32GetDatum(X)
#define TimeADTGetDatum(X)	  Float8GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)
#endif   /* HAVE_INT64_TIMESTAMP */

#define PG_GETARG_DATEADT(n)	 DatumGetDateADT(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMEADT(n)	 DatumGetTimeADT(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMETZADT_P(n) DatumGetTimeTzADTP(PG_GETARG_DATUM(n))

#define PG_RETURN_DATEADT(x)	 return DateADTGetDatum(x)
#define PG_RETURN_TIMEADT(x)	 return TimeADTGetDatum(x)
#define PG_RETURN_TIMETZADT_P(x) return TimeTzADTPGetDatum(x)


/* date.c */
extern double date2timestamp_no_overflow(DateADT dateVal);
extern void EncodeSpecialDate(DateADT dt, char *str);

extern Datum date_in(PG_FUNCTION_ARGS);
extern Datum date_out(PG_FUNCTION_ARGS);
extern Datum date_recv(PG_FUNCTION_ARGS);
extern Datum date_send(PG_FUNCTION_ARGS);
extern Datum make_date(PG_FUNCTION_ARGS);
extern Datum date_eq(PG_FUNCTION_ARGS);
extern Datum date_ne(PG_FUNCTION_ARGS);
extern Datum date_lt(PG_FUNCTION_ARGS);
extern Datum date_le(PG_FUNCTION_ARGS);
extern Datum date_gt(PG_FUNCTION_ARGS);
extern Datum date_ge(PG_FUNCTION_ARGS);
extern Datum date_cmp(PG_FUNCTION_ARGS);
extern Datum date_sortsupport(PG_FUNCTION_ARGS);
extern Datum date_finite(PG_FUNCTION_ARGS);
extern Datum date_larger(PG_FUNCTION_ARGS);
extern Datum date_smaller(PG_FUNCTION_ARGS);
extern Datum date_mi(PG_FUNCTION_ARGS);
extern Datum date_pli(PG_FUNCTION_ARGS);
extern Datum date_mii(PG_FUNCTION_ARGS);
extern Datum date_eq_timestamp(PG_FUNCTION_ARGS);
extern Datum date_ne_timestamp(PG_FUNCTION_ARGS);
extern Datum date_lt_timestamp(PG_FUNCTION_ARGS);
extern Datum date_le_timestamp(PG_FUNCTION_ARGS);
extern Datum date_gt_timestamp(PG_FUNCTION_ARGS);
extern Datum date_ge_timestamp(PG_FUNCTION_ARGS);
extern Datum date_cmp_timestamp(PG_FUNCTION_ARGS);
extern Datum date_eq_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_ne_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_lt_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_le_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_gt_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_ge_timestamptz(PG_FUNCTION_ARGS);
extern Datum date_cmp_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamp_eq_date(PG_FUNCTION_ARGS);
extern Datum timestamp_ne_date(PG_FUNCTION_ARGS);
extern Datum timestamp_lt_date(PG_FUNCTION_ARGS);
extern Datum timestamp_le_date(PG_FUNCTION_ARGS);
extern Datum timestamp_gt_date(PG_FUNCTION_ARGS);
extern Datum timestamp_ge_date(PG_FUNCTION_ARGS);
extern Datum timestamp_cmp_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_eq_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_ne_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_lt_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_le_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_gt_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_ge_date(PG_FUNCTION_ARGS);
extern Datum timestamptz_cmp_date(PG_FUNCTION_ARGS);
extern Datum date_pl_interval(PG_FUNCTION_ARGS);
extern Datum date_mi_interval(PG_FUNCTION_ARGS);
extern Datum date_timestamp(PG_FUNCTION_ARGS);
extern Datum timestamp_date(PG_FUNCTION_ARGS);
extern Datum date_timestamptz(PG_FUNCTION_ARGS);
extern Datum timestamptz_date(PG_FUNCTION_ARGS);
extern Datum datetime_timestamp(PG_FUNCTION_ARGS);
extern Datum abstime_date(PG_FUNCTION_ARGS);

extern Datum time_in(PG_FUNCTION_ARGS);
extern Datum time_out(PG_FUNCTION_ARGS);
extern Datum time_recv(PG_FUNCTION_ARGS);
extern Datum time_send(PG_FUNCTION_ARGS);
extern Datum timetypmodin(PG_FUNCTION_ARGS);
extern Datum timetypmodout(PG_FUNCTION_ARGS);
extern Datum make_time(PG_FUNCTION_ARGS);
extern Datum time_transform(PG_FUNCTION_ARGS);
extern Datum time_scale(PG_FUNCTION_ARGS);
extern Datum time_eq(PG_FUNCTION_ARGS);
extern Datum time_ne(PG_FUNCTION_ARGS);
extern Datum time_lt(PG_FUNCTION_ARGS);
extern Datum time_le(PG_FUNCTION_ARGS);
extern Datum time_gt(PG_FUNCTION_ARGS);
extern Datum time_ge(PG_FUNCTION_ARGS);
extern Datum time_cmp(PG_FUNCTION_ARGS);
extern Datum time_hash(PG_FUNCTION_ARGS);
extern Datum overlaps_time(PG_FUNCTION_ARGS);
extern Datum time_larger(PG_FUNCTION_ARGS);
extern Datum time_smaller(PG_FUNCTION_ARGS);
extern Datum time_mi_time(PG_FUNCTION_ARGS);
extern Datum timestamp_time(PG_FUNCTION_ARGS);
extern Datum timestamptz_time(PG_FUNCTION_ARGS);
extern Datum time_interval(PG_FUNCTION_ARGS);
extern Datum interval_time(PG_FUNCTION_ARGS);
extern Datum time_pl_interval(PG_FUNCTION_ARGS);
extern Datum time_mi_interval(PG_FUNCTION_ARGS);
extern Datum time_part(PG_FUNCTION_ARGS);

extern Datum timetz_in(PG_FUNCTION_ARGS);
extern Datum timetz_out(PG_FUNCTION_ARGS);
extern Datum timetz_recv(PG_FUNCTION_ARGS);
extern Datum timetz_send(PG_FUNCTION_ARGS);
extern Datum timetztypmodin(PG_FUNCTION_ARGS);
extern Datum timetztypmodout(PG_FUNCTION_ARGS);
extern Datum timetz_scale(PG_FUNCTION_ARGS);
extern Datum timetz_eq(PG_FUNCTION_ARGS);
extern Datum timetz_ne(PG_FUNCTION_ARGS);
extern Datum timetz_lt(PG_FUNCTION_ARGS);
extern Datum timetz_le(PG_FUNCTION_ARGS);
extern Datum timetz_gt(PG_FUNCTION_ARGS);
extern Datum timetz_ge(PG_FUNCTION_ARGS);
extern Datum timetz_cmp(PG_FUNCTION_ARGS);
extern Datum timetz_hash(PG_FUNCTION_ARGS);
extern Datum overlaps_timetz(PG_FUNCTION_ARGS);
extern Datum timetz_larger(PG_FUNCTION_ARGS);
extern Datum timetz_smaller(PG_FUNCTION_ARGS);
extern Datum timetz_time(PG_FUNCTION_ARGS);
extern Datum time_timetz(PG_FUNCTION_ARGS);
extern Datum timestamptz_timetz(PG_FUNCTION_ARGS);
extern Datum datetimetz_timestamptz(PG_FUNCTION_ARGS);
extern Datum timetz_part(PG_FUNCTION_ARGS);
extern Datum timetz_zone(PG_FUNCTION_ARGS);
extern Datum timetz_izone(PG_FUNCTION_ARGS);
extern Datum timetz_pl_interval(PG_FUNCTION_ARGS);
extern Datum timetz_mi_interval(PG_FUNCTION_ARGS);

#endif   /* DATE_H */
