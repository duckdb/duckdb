/*-------------------------------------------------------------------------
 *
 * skey.h
 *	  POSTGRES scan key definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/skey.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SKEY_H
#define SKEY_H

#include "access/attnum.h"
#include "access/stratnum.h"
#include "fmgr.h"


/*
 * A ScanKey represents the application of a comparison operator between
 * a table or index column and a constant.  When it's part of an array of
 * ScanKeys, the comparison conditions are implicitly ANDed.  The index
 * column is the left argument of the operator, if it's a binary operator.
 * (The data structure can support unary indexable operators too; in that
 * case sk_argument would go unused.  This is not currently implemented.)
 *
 * For an index scan, sk_strategy and sk_subtype must be set correctly for
 * the operator.  When using a ScanKey in a heap scan, these fields are not
 * used and may be set to InvalidStrategy/InvalidOid.
 *
 * If the operator is collation-sensitive, sk_collation must be set
 * correctly as well.
 *
 * A ScanKey can also represent a ScalarArrayOpExpr, that is a condition
 * "column op ANY(ARRAY[...])".  This is signaled by the SK_SEARCHARRAY
 * flag bit.  The sk_argument is not a value of the operator's right-hand
 * argument type, but rather an array of such values, and the per-element
 * comparisons are to be ORed together.
 *
 * A ScanKey can also represent a condition "column IS NULL" or "column
 * IS NOT NULL"; these cases are signaled by the SK_SEARCHNULL and
 * SK_SEARCHNOTNULL flag bits respectively.  The argument is always NULL,
 * and the sk_strategy, sk_subtype, sk_collation, and sk_func fields are
 * not used (unless set by the index AM).
 *
 * SK_SEARCHARRAY, SK_SEARCHNULL and SK_SEARCHNOTNULL are supported only
 * for index scans, not heap scans; and not all index AMs support them,
 * only those that set amsearcharray or amsearchnulls respectively.
 *
 * A ScanKey can also represent an ordering operator invocation, that is
 * an ordering requirement "ORDER BY indexedcol op constant".  This looks
 * the same as a comparison operator, except that the operator doesn't
 * (usually) yield boolean.  We mark such ScanKeys with SK_ORDER_BY.
 * SK_SEARCHARRAY, SK_SEARCHNULL, SK_SEARCHNOTNULL cannot be used here.
 *
 * Note: in some places, ScanKeys are used as a convenient representation
 * for the invocation of an access method support procedure.  In this case
 * sk_strategy/sk_subtype are not meaningful (but sk_collation can be); and
 * sk_func may refer to a function that returns something other than boolean.
 */
typedef struct ScanKeyData
{
	int			sk_flags;		/* flags, see below */
	AttrNumber	sk_attno;		/* table or index column number */
	StrategyNumber sk_strategy; /* operator strategy number */
	Oid			sk_subtype;		/* strategy subtype */
	Oid			sk_collation;	/* collation to use, if needed */
	FmgrInfo	sk_func;		/* lookup info for function to call */
	Datum		sk_argument;	/* data to compare */
} ScanKeyData;

typedef ScanKeyData *ScanKey;

/*
 * About row comparisons:
 *
 * The ScanKey data structure also supports row comparisons, that is ordered
 * tuple comparisons like (x, y) > (c1, c2), having the SQL-spec semantics
 * "x > c1 OR (x = c1 AND y > c2)".  Note that this is currently only
 * implemented for btree index searches, not for heapscans or any other index
 * type.  A row comparison is represented by a "header" ScanKey entry plus
 * a separate array of ScanKeys, one for each column of the row comparison.
 * The header entry has these properties:
 *		sk_flags = SK_ROW_HEADER
 *		sk_attno = index column number for leading column of row comparison
 *		sk_strategy = btree strategy code for semantics of row comparison
 *				(ie, < <= > or >=)
 *		sk_subtype, sk_collation, sk_func: not used
 *		sk_argument: pointer to subsidiary ScanKey array
 * If the header is part of a ScanKey array that's sorted by attno, it
 * must be sorted according to the leading column number.
 *
 * The subsidiary ScanKey array appears in logical column order of the row
 * comparison, which may be different from index column order.  The array
 * elements are like a normal ScanKey array except that:
 *		sk_flags must include SK_ROW_MEMBER, plus SK_ROW_END in the last
 *				element (needed since row header does not include a count)
 *		sk_func points to the btree comparison support function for the
 *				opclass, NOT the operator's implementation function.
 * sk_strategy must be the same in all elements of the subsidiary array,
 * that is, the same as in the header entry.
 * SK_SEARCHARRAY, SK_SEARCHNULL, SK_SEARCHNOTNULL cannot be used here.
 */

/*
 * ScanKeyData sk_flags
 *
 * sk_flags bits 0-15 are reserved for system-wide use (symbols for those
 * bits should be defined here).  Bits 16-31 are reserved for use within
 * individual index access methods.
 */
#define SK_ISNULL			0x0001		/* sk_argument is NULL */
#define SK_UNARY			0x0002		/* unary operator (not supported!) */
#define SK_ROW_HEADER		0x0004		/* row comparison header (see above) */
#define SK_ROW_MEMBER		0x0008		/* row comparison member (see above) */
#define SK_ROW_END			0x0010		/* last row comparison member */
#define SK_SEARCHARRAY		0x0020		/* scankey represents ScalarArrayOp */
#define SK_SEARCHNULL		0x0040		/* scankey represents "col IS NULL" */
#define SK_SEARCHNOTNULL	0x0080		/* scankey represents "col IS NOT
										 * NULL" */
#define SK_ORDER_BY			0x0100		/* scankey is for ORDER BY op */


/*
 * prototypes for functions in access/common/scankey.c
 */
extern void ScanKeyInit(ScanKey entry,
			AttrNumber attributeNumber,
			StrategyNumber strategy,
			RegProcedure procedure,
			Datum argument);
extern void ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument);
extern void ScanKeyEntryInitializeWithInfo(ScanKey entry,
							   int flags,
							   AttrNumber attributeNumber,
							   StrategyNumber strategy,
							   Oid subtype,
							   Oid collation,
							   FmgrInfo *finfo,
							   Datum argument);

#endif   /* SKEY_H */
