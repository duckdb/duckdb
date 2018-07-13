/*-------------------------------------------------------------------------
 *
 * off.h
 *	  POSTGRES disk "offset" definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/off.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OFF_H
#define OFF_H

#include "storage/itemid.h"
/*
 * OffsetNumber:
 *
 * this is a 1-based index into the linp (ItemIdData) array in the
 * header of each disk page.
 */
typedef uint16 OffsetNumber;

#define InvalidOffsetNumber		((OffsetNumber) 0)
#define FirstOffsetNumber		((OffsetNumber) 1)
#define MaxOffsetNumber			((OffsetNumber) (BLCKSZ / sizeof(ItemIdData)))
#define OffsetNumberMask		(0xffff)		/* valid uint16 bits */

/* ----------------
 *		support macros
 * ----------------
 */

/*
 * OffsetNumberIsValid
 *		True iff the offset number is valid.
 */
#define OffsetNumberIsValid(offsetNumber) \
	((bool) ((offsetNumber != InvalidOffsetNumber) && \
			 (offsetNumber <= MaxOffsetNumber)))

/*
 * OffsetNumberNext
 * OffsetNumberPrev
 *		Increments/decrements the argument.  These macros look pointless
 *		but they help us disambiguate the different manipulations on
 *		OffsetNumbers (e.g., sometimes we subtract one from an
 *		OffsetNumber to move back, and sometimes we do so to form a
 *		real C array index).
 */
#define OffsetNumberNext(offsetNumber) \
	((OffsetNumber) (1 + (offsetNumber)))
#define OffsetNumberPrev(offsetNumber) \
	((OffsetNumber) (-1 + (offsetNumber)))

#endif   /* OFF_H */
