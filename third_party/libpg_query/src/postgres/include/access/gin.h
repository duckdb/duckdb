/*--------------------------------------------------------------------------
 * gin.h
 *	  Public header file for Generalized Inverted Index access method.
 *
 *	Copyright (c) 2006-2015, PostgreSQL Global Development Group
 *
 *	src/include/access/gin.h
 *--------------------------------------------------------------------------
 */
#ifndef GIN_H
#define GIN_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "utils/relcache.h"


/*
 * amproc indexes for inverted indexes.
 */
#define GIN_COMPARE_PROC			   1
#define GIN_EXTRACTVALUE_PROC		   2
#define GIN_EXTRACTQUERY_PROC		   3
#define GIN_CONSISTENT_PROC			   4
#define GIN_COMPARE_PARTIAL_PROC	   5
#define GIN_TRICONSISTENT_PROC		   6
#define GINNProcs					   6

/*
 * searchMode settings for extractQueryFn.
 */
#define GIN_SEARCH_MODE_DEFAULT			0
#define GIN_SEARCH_MODE_INCLUDE_EMPTY	1
#define GIN_SEARCH_MODE_ALL				2
#define GIN_SEARCH_MODE_EVERYTHING		3		/* for internal use only */

/*
 * GinStatsData represents stats data for planner use
 */
typedef struct GinStatsData
{
	BlockNumber nPendingPages;
	BlockNumber nTotalPages;
	BlockNumber nEntryPages;
	BlockNumber nDataPages;
	int64		nEntries;
	int32		ginVersion;
} GinStatsData;

/*
 * A ternary value used by tri-consistent functions.
 *
 * For convenience, this is compatible with booleans. A boolean can be
 * safely cast to a GinTernaryValue.
 */
typedef char GinTernaryValue;

#define GIN_FALSE		0		/* item is not present / does not match */
#define GIN_TRUE		1		/* item is present / matches */
#define GIN_MAYBE		2		/* don't know if item is present / don't know
								 * if matches */

#define DatumGetGinTernaryValue(X) ((GinTernaryValue)(X))
#define GinTernaryValueGetDatum(X) ((Datum)(X))
#define PG_RETURN_GIN_TERNARY_VALUE(x) return GinTernaryValueGetDatum(x)

/* GUC parameters */
extern PGDLLIMPORT int GinFuzzySearchLimit;
extern int	gin_pending_list_limit;

/* ginutil.c */
extern void ginGetStats(Relation index, GinStatsData *stats);
extern void ginUpdateStats(Relation index, const GinStatsData *stats);

/* ginxlog.c */
extern void gin_redo(XLogReaderState *record);
extern void gin_desc(StringInfo buf, XLogReaderState *record);
extern const char *gin_identify(uint8 info);
extern void gin_xlog_startup(void);
extern void gin_xlog_cleanup(void);

#endif   /* GIN_H */
