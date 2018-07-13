/*-------------------------------------------------------------------------
 *
 * sortsupport.h
 *	  Framework for accelerated sorting.
 *
 * Traditionally, PostgreSQL has implemented sorting by repeatedly invoking
 * an SQL-callable comparison function "cmp(x, y) returns int" on pairs of
 * values to be compared, where the comparison function is the BTORDER_PROC
 * pg_amproc support function of the appropriate btree index opclass.
 *
 * This file defines alternative APIs that allow sorting to be performed with
 * reduced overhead.  To support lower-overhead sorting, a btree opclass may
 * provide a BTSORTSUPPORT_PROC pg_amproc entry, which must take a single
 * argument of type internal and return void.  The argument is actually a
 * pointer to a SortSupportData struct, which is defined below.
 *
 * If provided, the BTSORTSUPPORT function will be called during sort setup,
 * and it must initialize the provided struct with pointers to function(s)
 * that can be called to perform sorting.  This API is defined to allow
 * multiple acceleration mechanisms to be supported, but no opclass is
 * required to provide all of them.  The BTSORTSUPPORT function should
 * simply not set any function pointers for mechanisms it doesn't support.
 * Opclasses that provide BTSORTSUPPORT and don't provide a comparator
 * function will have a shim set up by sort support automatically.  However,
 * opclasses that support the optional additional abbreviated key capability
 * must always provide an authoritative comparator used to tie-break
 * inconclusive abbreviated comparisons and also used  when aborting
 * abbreviation.  Furthermore, a converter and abort/costing function must be
 * provided.
 *
 * All sort support functions will be passed the address of the
 * SortSupportData struct when called, so they can use it to store
 * additional private data as needed.  In particular, for collation-aware
 * datatypes, the ssup_collation field is set before calling BTSORTSUPPORT
 * and is available to all support functions.  Additional opclass-dependent
 * data can be stored using the ssup_extra field.  Any such data
 * should be allocated in the ssup_cxt memory context.
 *
 * Note: since pg_amproc functions are indexed by (lefttype, righttype)
 * it is possible to associate a BTSORTSUPPORT function with a cross-type
 * comparison.  This could sensibly be used to provide a fast comparator
 * function for such cases, but probably not any other acceleration method.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/sortsupport.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SORTSUPPORT_H
#define SORTSUPPORT_H

#include "access/attnum.h"
#include "utils/relcache.h"

typedef struct SortSupportData *SortSupport;

typedef struct SortSupportData
{
	/*
	 * These fields are initialized before calling the BTSORTSUPPORT function
	 * and should not be changed later.
	 */
	MemoryContext ssup_cxt;		/* Context containing sort info */
	Oid			ssup_collation; /* Collation to use, or InvalidOid */

	/*
	 * Additional sorting parameters; but unlike ssup_collation, these can be
	 * changed after BTSORTSUPPORT is called, so don't use them in selecting
	 * sort support functions.
	 */
	bool		ssup_reverse;	/* descending-order sort? */
	bool		ssup_nulls_first;		/* sort nulls first? */

	/*
	 * These fields are workspace for callers, and should not be touched by
	 * opclass-specific functions.
	 */
	AttrNumber	ssup_attno;		/* column number to sort */

	/*
	 * ssup_extra is zeroed before calling the BTSORTSUPPORT function, and is
	 * not touched subsequently by callers.
	 */
	void	   *ssup_extra;		/* Workspace for opclass functions */

	/*
	 * Function pointers are zeroed before calling the BTSORTSUPPORT function,
	 * and must be set by it for any acceleration methods it wants to supply.
	 * The comparator pointer must be set, others are optional.
	 */

	/*
	 * Comparator function has the same API as the traditional btree
	 * comparison function, ie, return <0, 0, or >0 according as x is less
	 * than, equal to, or greater than y.  Note that x and y are guaranteed
	 * not null, and there is no way to return null either.  Do not return
	 * INT_MIN, as callers are allowed to negate the result before using it.
	 *
	 * This may be either the authoritative comparator, or the abbreviated
	 * comparator.  Core code may switch this over the initial preference of
	 * an opclass support function despite originally indicating abbreviation
	 * was applicable, by assigning the authoritative comparator back.
	 */
	int			(*comparator) (Datum x, Datum y, SortSupport ssup);

	/*
	 * "Abbreviated key" infrastructure follows.
	 *
	 * All callbacks must be set by sortsupport opclasses that make use of
	 * this optional additional infrastructure (unless for whatever reasons
	 * the opclass doesn't proceed with abbreviation, in which case
	 * abbrev_converter must not be set).
	 *
	 * This allows opclass authors to supply a conversion routine, used to
	 * create an alternative representation of the underlying type (an
	 * "abbreviated key").  This representation must be pass-by-value and
	 * typically will use some ad-hoc format that only the opclass has
	 * knowledge of.  An alternative comparator, used only with this
	 * alternative representation must also be provided (which is assigned to
	 * "comparator").  This representation is a simple approximation of the
	 * original Datum.  It must be possible to compare datums of this
	 * representation with each other using the supplied alternative
	 * comparator, and have any non-zero return value be a reliable proxy for
	 * what a proper comparison would indicate. Returning zero from the
	 * alternative comparator does not indicate equality, as with a
	 * conventional support routine 1, though -- it indicates that it wasn't
	 * possible to determine how the two abbreviated values compared.  A
	 * proper comparison, using "abbrev_full_comparator"/
	 * ApplySortAbbrevFullComparator() is therefore required.  In many cases
	 * this results in most or all comparisons only using the cheap
	 * alternative comparison func, which is typically implemented as code
	 * that compiles to just a few CPU instructions.  CPU cache miss penalties
	 * are expensive; to get good overall performance, sort infrastructure
	 * must heavily weigh cache performance.
	 *
	 * Opclass authors must consider the final cardinality of abbreviated keys
	 * when devising an encoding scheme.  It's possible for a strategy to work
	 * better than an alternative strategy with one usage pattern, while the
	 * reverse might be true for another usage pattern.  All of these factors
	 * must be considered.
	 */

	/*
	 * "abbreviate" concerns whether or not the abbreviated key optimization
	 * is applicable in principle (that is, the sortsupport routine needs to
	 * know if its dealing with a key where an abbreviated representation can
	 * usefully be packed together.  Conventionally, this is the leading
	 * attribute key).  Note, however, that in order to determine that
	 * abbreviation is not in play, the core code always checks whether or not
	 * the opclass has set abbrev_converter.  This is a one way, one time
	 * message to the opclass.
	 */
	bool		abbreviate;

	/*
	 * Converter to abbreviated format, from original representation.  Core
	 * code uses this callback to convert from a pass-by-reference "original"
	 * Datum to a pass-by-value abbreviated key Datum.  Note that original is
	 * guaranteed NOT NULL, because it doesn't make sense to factor NULLness
	 * into ad-hoc cost model.
	 *
	 * abbrev_converter is tested to see if abbreviation is in play.  Core
	 * code may set it to NULL to indicate abbreviation should not be used
	 * (which is something sortsupport routines need not concern themselves
	 * with). However, sortsupport routines must not set it when it is
	 * immediately established that abbreviation should not proceed (e.g., for
	 * !abbreviate calls, or due to platform-specific impediments to using
	 * abbreviation).
	 */
	Datum		(*abbrev_converter) (Datum original, SortSupport ssup);

	/*
	 * abbrev_abort callback allows clients to verify that the current
	 * strategy is working out, using a sortsupport routine defined ad-hoc
	 * cost model. If there is a lot of duplicate abbreviated keys in
	 * practice, it's useful to be able to abandon the strategy before paying
	 * too high a cost in conversion (perhaps certain opclass-specific
	 * adaptations are useful too).
	 */
	bool		(*abbrev_abort) (int memtupcount, SortSupport ssup);

	/*
	 * Full, authoritative comparator for key that an abbreviated
	 * representation was generated for, used when an abbreviated comparison
	 * was inconclusive (by calling ApplySortComparatorFull()), or used to
	 * replace "comparator" when core system ultimately decides against
	 * abbreviation.
	 */
	int			(*abbrev_full_comparator) (Datum x, Datum y, SortSupport ssup);
} SortSupportData;


/*
 * ApplySortComparator should be inlined if possible.  See STATIC_IF_INLINE
 * in c.h.
 */
#ifndef PG_USE_INLINE
extern int ApplySortComparator(Datum datum1, bool isNull1,
					Datum datum2, bool isNull2,
					SortSupport ssup);
extern int ApplySortAbbrevFullComparator(Datum datum1, bool isNull1,
							  Datum datum2, bool isNull2,
							  SortSupport ssup);
#endif   /* !PG_USE_INLINE */
#if defined(PG_USE_INLINE) || defined(SORTSUPPORT_INCLUDE_DEFINITIONS)
/*
 * Apply a sort comparator function and return a 3-way comparison result.
 * This takes care of handling reverse-sort and NULLs-ordering properly.
 */
STATIC_IF_INLINE int
ApplySortComparator(Datum datum1, bool isNull1,
					Datum datum2, bool isNull2,
					SortSupport ssup)
{
	int			compare;

	if (isNull1)
	{
		if (isNull2)
			compare = 0;		/* NULL "=" NULL */
		else if (ssup->ssup_nulls_first)
			compare = -1;		/* NULL "<" NOT_NULL */
		else
			compare = 1;		/* NULL ">" NOT_NULL */
	}
	else if (isNull2)
	{
		if (ssup->ssup_nulls_first)
			compare = 1;		/* NOT_NULL ">" NULL */
		else
			compare = -1;		/* NOT_NULL "<" NULL */
	}
	else
	{
		compare = (*ssup->comparator) (datum1, datum2, ssup);
		if (ssup->ssup_reverse)
			compare = -compare;
	}

	return compare;
}

/*
 * Apply a sort comparator function and return a 3-way comparison using full,
 * authoritative comparator.  This takes care of handling reverse-sort and
 * NULLs-ordering properly.
 */
STATIC_IF_INLINE int
ApplySortAbbrevFullComparator(Datum datum1, bool isNull1,
							  Datum datum2, bool isNull2,
							  SortSupport ssup)
{
	int			compare;

	if (isNull1)
	{
		if (isNull2)
			compare = 0;		/* NULL "=" NULL */
		else if (ssup->ssup_nulls_first)
			compare = -1;		/* NULL "<" NOT_NULL */
		else
			compare = 1;		/* NULL ">" NOT_NULL */
	}
	else if (isNull2)
	{
		if (ssup->ssup_nulls_first)
			compare = 1;		/* NOT_NULL ">" NULL */
		else
			compare = -1;		/* NOT_NULL "<" NULL */
	}
	else
	{
		compare = (*ssup->abbrev_full_comparator) (datum1, datum2, ssup);
		if (ssup->ssup_reverse)
			compare = -compare;
	}

	return compare;
}
#endif   /*-- PG_USE_INLINE || SORTSUPPORT_INCLUDE_DEFINITIONS */

/* Other functions in utils/sort/sortsupport.c */
extern void PrepareSortSupportComparisonShim(Oid cmpFunc, SortSupport ssup);
extern void PrepareSortSupportFromOrderingOp(Oid orderingOp, SortSupport ssup);
extern void PrepareSortSupportFromIndexRel(Relation indexRel, int16 strategy,
							   SortSupport ssup);

#endif   /* SORTSUPPORT_H */
