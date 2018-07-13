/*-------------------------------------------------------------------------
 *
 * tidbitmap.h
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 *
 * Copyright (c) 2003-2015, PostgreSQL Global Development Group
 *
 * src/include/nodes/tidbitmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDBITMAP_H
#define TIDBITMAP_H

#include "storage/itemptr.h"


/*
 * Actual bitmap representation is private to tidbitmap.c.  Callers can
 * do IsA(x, TIDBitmap) on it, but nothing else.
 */
typedef struct TIDBitmap TIDBitmap;

/* Likewise, TBMIterator is private */
typedef struct TBMIterator TBMIterator;

/* Result structure for tbm_iterate */
typedef struct
{
	BlockNumber blockno;		/* page number containing tuples */
	int			ntuples;		/* -1 indicates lossy result */
	bool		recheck;		/* should the tuples be rechecked? */
	/* Note: recheck is always true if ntuples < 0 */
	OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
} TBMIterateResult;

/* function prototypes in nodes/tidbitmap.c */

extern TIDBitmap *tbm_create(long maxbytes);
extern void tbm_free(TIDBitmap *tbm);

extern void tbm_add_tuples(TIDBitmap *tbm,
			   const ItemPointer tids, int ntids,
			   bool recheck);
extern void tbm_add_page(TIDBitmap *tbm, BlockNumber pageno);

extern void tbm_union(TIDBitmap *a, const TIDBitmap *b);
extern void tbm_intersect(TIDBitmap *a, const TIDBitmap *b);

extern bool tbm_is_empty(const TIDBitmap *tbm);

extern TBMIterator *tbm_begin_iterate(TIDBitmap *tbm);
extern TBMIterateResult *tbm_iterate(TBMIterator *iterator);
extern void tbm_end_iterate(TBMIterator *iterator);

#endif   /* TIDBITMAP_H */
