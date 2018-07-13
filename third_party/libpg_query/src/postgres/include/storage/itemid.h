/*-------------------------------------------------------------------------
 *
 * itemid.h
 *	  Standard POSTGRES buffer page item identifier definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/itemid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ITEMID_H
#define ITEMID_H

/*
 * An item pointer (also called line pointer) on a buffer page
 *
 * In some cases an item pointer is "in use" but does not have any associated
 * storage on the page.  By convention, lp_len == 0 in every item pointer
 * that does not have storage, independently of its lp_flags state.
 */
typedef struct ItemIdData
{
	unsigned	lp_off:15,		/* offset to tuple (from start of page) */
				lp_flags:2,		/* state of item pointer, see below */
				lp_len:15;		/* byte length of tuple */
} ItemIdData;

typedef ItemIdData *ItemId;

/*
 * lp_flags has these possible states.  An UNUSED line pointer is available
 * for immediate re-use, the other states are not.
 */
#define LP_UNUSED		0		/* unused (should always have lp_len=0) */
#define LP_NORMAL		1		/* used (should always have lp_len>0) */
#define LP_REDIRECT		2		/* HOT redirect (should have lp_len=0) */
#define LP_DEAD			3		/* dead, may or may not have storage */

/*
 * Item offsets and lengths are represented by these types when
 * they're not actually stored in an ItemIdData.
 */
typedef uint16 ItemOffset;
typedef uint16 ItemLength;


/* ----------------
 *		support macros
 * ----------------
 */

/*
 *		ItemIdGetLength
 */
#define ItemIdGetLength(itemId) \
   ((itemId)->lp_len)

/*
 *		ItemIdGetOffset
 */
#define ItemIdGetOffset(itemId) \
   ((itemId)->lp_off)

/*
 *		ItemIdGetFlags
 */
#define ItemIdGetFlags(itemId) \
   ((itemId)->lp_flags)

/*
 *		ItemIdGetRedirect
 * In a REDIRECT pointer, lp_off holds the link to the next item pointer
 */
#define ItemIdGetRedirect(itemId) \
   ((itemId)->lp_off)

/*
 * ItemIdIsValid
 *		True iff item identifier is valid.
 *		This is a pretty weak test, probably useful only in Asserts.
 */
#define ItemIdIsValid(itemId)	PointerIsValid(itemId)

/*
 * ItemIdIsUsed
 *		True iff item identifier is in use.
 */
#define ItemIdIsUsed(itemId) \
	((itemId)->lp_flags != LP_UNUSED)

/*
 * ItemIdIsNormal
 *		True iff item identifier is in state NORMAL.
 */
#define ItemIdIsNormal(itemId) \
	((itemId)->lp_flags == LP_NORMAL)

/*
 * ItemIdIsRedirected
 *		True iff item identifier is in state REDIRECT.
 */
#define ItemIdIsRedirected(itemId) \
	((itemId)->lp_flags == LP_REDIRECT)

/*
 * ItemIdIsDead
 *		True iff item identifier is in state DEAD.
 */
#define ItemIdIsDead(itemId) \
	((itemId)->lp_flags == LP_DEAD)

/*
 * ItemIdHasStorage
 *		True iff item identifier has associated storage.
 */
#define ItemIdHasStorage(itemId) \
	((itemId)->lp_len != 0)

/*
 * ItemIdSetUnused
 *		Set the item identifier to be UNUSED, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetUnused(itemId) \
( \
	(itemId)->lp_flags = LP_UNUSED, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetNormal
 *		Set the item identifier to be NORMAL, with the specified storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetNormal(itemId, off, len) \
( \
	(itemId)->lp_flags = LP_NORMAL, \
	(itemId)->lp_off = (off), \
	(itemId)->lp_len = (len) \
)

/*
 * ItemIdSetRedirect
 *		Set the item identifier to be REDIRECT, with the specified link.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetRedirect(itemId, link) \
( \
	(itemId)->lp_flags = LP_REDIRECT, \
	(itemId)->lp_off = (link), \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetDead
 *		Set the item identifier to be DEAD, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetDead(itemId) \
( \
	(itemId)->lp_flags = LP_DEAD, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdMarkDead
 *		Set the item identifier to be DEAD, keeping its existing storage.
 *
 * Note: in indexes, this is used as if it were a hint-bit mechanism;
 * we trust that multiple processors can do this in parallel and get
 * the same result.
 */
#define ItemIdMarkDead(itemId) \
( \
	(itemId)->lp_flags = LP_DEAD \
)

#endif   /* ITEMID_H */
