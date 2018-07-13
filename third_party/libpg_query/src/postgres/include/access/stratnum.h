/*-------------------------------------------------------------------------
 *
 * stratnum.h
 *	  POSTGRES strategy number definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/stratnum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRATNUM_H
#define STRATNUM_H

/*
 * Strategy numbers identify the semantics that particular operators have
 * with respect to particular operator classes.  In some cases a strategy
 * subtype (an OID) is used as further information.
 */
typedef uint16 StrategyNumber;

#define InvalidStrategy ((StrategyNumber) 0)

/*
 * Strategy numbers for B-tree indexes.
 */
#define BTLessStrategyNumber			1
#define BTLessEqualStrategyNumber		2
#define BTEqualStrategyNumber			3
#define BTGreaterEqualStrategyNumber	4
#define BTGreaterStrategyNumber			5

#define BTMaxStrategyNumber				5


/*
 * Strategy numbers common to (some) GiST, SP-GiST and BRIN opclasses.
 *
 * The first few of these come from the R-Tree indexing method (hence the
 * names); the others have been added over time as they have been needed.
 */
#define RTLeftStrategyNumber			1		/* for << */
#define RTOverLeftStrategyNumber		2		/* for &< */
#define RTOverlapStrategyNumber			3		/* for && */
#define RTOverRightStrategyNumber		4		/* for &> */
#define RTRightStrategyNumber			5		/* for >> */
#define RTSameStrategyNumber			6		/* for ~= */
#define RTContainsStrategyNumber		7		/* for @> */
#define RTContainedByStrategyNumber		8		/* for <@ */
#define RTOverBelowStrategyNumber		9		/* for &<| */
#define RTBelowStrategyNumber			10		/* for <<| */
#define RTAboveStrategyNumber			11		/* for |>> */
#define RTOverAboveStrategyNumber		12		/* for |&> */
#define RTOldContainsStrategyNumber		13		/* for old spelling of @> */
#define RTOldContainedByStrategyNumber	14		/* for old spelling of <@ */
#define RTKNNSearchStrategyNumber		15		/* for <-> (distance) */
#define RTContainsElemStrategyNumber	16		/* for range types @> elem */
#define RTAdjacentStrategyNumber		17		/* for -|- */
#define RTEqualStrategyNumber			18		/* for = */
#define RTNotEqualStrategyNumber		19		/* for != */
#define RTLessStrategyNumber			20		/* for < */
#define RTLessEqualStrategyNumber		21		/* for <= */
#define RTGreaterStrategyNumber			22		/* for > */
#define RTGreaterEqualStrategyNumber	23		/* for >= */
#define RTSubStrategyNumber				24		/* for inet >> */
#define RTSubEqualStrategyNumber		25		/* for inet <<= */
#define RTSuperStrategyNumber			26		/* for inet << */
#define RTSuperEqualStrategyNumber		27		/* for inet >>= */

#define RTMaxStrategyNumber				27


#endif   /* STRATNUM_H */
