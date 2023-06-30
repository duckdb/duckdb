//---------------------------------------------------------------------------
//	@filename:
//		CRange.h
//
//	@doc:
//		Representation of a range of values
//---------------------------------------------------------------------------
#ifndef GPOPT_CRange_H
#define GPOPT_CRange_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/types.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpnaucrates
{
// fwd declarations
class IDatum;
}  // namespace gpnaucrates

namespace gpopt
{
using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;

//fwd declarations

class CRange;
class IComparator;

//---------------------------------------------------------------------------
//	@class:
//		CRange
//
//	@doc:
//		Representation of a range of values
//
//---------------------------------------------------------------------------
class CRange : public CRefCount
{
public:
	enum ERangeInclusion
	{
		EriIncluded,
		EriExcluded,
		EriSentinel
	};

private:
	// range type
	IMDId *m_mdid;

	// datum comparator
	const IComparator *m_pcomp;

	// left end point, NULL if infinite
	IDatum *m_pdatumLeft;

	// inclusion option for left end
	ERangeInclusion m_eriLeft;

	// right end point, NULL if infinite
	IDatum *m_pdatumRight;

	// inclusion option for right end
	ERangeInclusion m_eriRight;

	// hidden copy ctor
	CRange(const CRange &);

	// construct an equality predicate if possible
	CExpression *PexprEquality(CMemoryPool *mp, const CColRef *colref);

	// construct a scalar comparison expression from one of the ends
	CExpression *PexprScalarCompEnd(CMemoryPool *mp, IDatum *datum,
									ERangeInclusion eri,
									IMDType::ECmpType ecmptIncl,
									IMDType::ECmpType ecmptExcl,
									const CColRef *colref);

	// inverse of the inclusion option
	ERangeInclusion
	EriInverseInclusion(ERangeInclusion eri)
	{
		if (EriIncluded == eri)
		{
			return EriExcluded;
		}
		return EriIncluded;
	}

	// print a bound
	IOstream &OsPrintBound(IOstream &os, IDatum *datum,
						   const CHAR *szInfinity) const;

public:
	// ctor
	CRange(IMDId *mdid, const IComparator *pcomp, IDatum *pdatumLeft,
		   ERangeInclusion eriLeft, IDatum *pdatumRight,
		   ERangeInclusion eriRight);

	// ctor
	CRange(const IComparator *pcomp, IMDType::ECmpType cmp_type, IDatum *datum);

	// dtor
	virtual ~CRange();

	// range type
	IMDId *
	MDId() const
	{
		return m_mdid;
	}

	// range beginning
	IDatum *
	PdatumLeft() const
	{
		return m_pdatumLeft;
	}

	// range end
	IDatum *
	PdatumRight() const
	{
		return m_pdatumRight;
	}

	// left end inclusion
	ERangeInclusion
	EriLeft() const
	{
		return m_eriLeft;
	}

	// right end inclusion
	ERangeInclusion
	EriRight() const
	{
		return m_eriRight;
	}

	// is this range disjoint from the given range and to its left
	BOOL FDisjointLeft(CRange *prange);

	// does this range contain the given range
	BOOL Contains(CRange *prange);

	// does this range overlap only the left end of the given range
	BOOL FOverlapsLeft(CRange *prange);

	// does this range overlap only the right end of the given range
	BOOL FOverlapsRight(CRange *prange);

	// does this range's upper bound equal the given range's lower bound
	BOOL FUpperBoundEqualsLowerBound(CRange *prange);

	// does this range start before the given range starts
	BOOL FStartsBefore(CRange *prange);

	// does this range start with or before the given range
	BOOL FStartsWithOrBefore(CRange *prange);

	// does this range end after the given range ends
	BOOL FEndsAfter(CRange *prange);

	// does this range end with or after the given range
	BOOL FEndsWithOrAfter(CRange *prange);

	// check if range represents a point
	BOOL FPoint() const;

	// intersection with another range
	CRange *PrngIntersect(CMemoryPool *mp, CRange *prange);

	// difference between this range and a given range on the left side only
	CRange *PrngDifferenceLeft(CMemoryPool *mp, CRange *prange);

	// difference between this range and a given range on the right side only
	CRange *PrngDifferenceRight(CMemoryPool *mp, CRange *prange);

	// return the extension of this range with the given range. The given
	// range must start right after this range, otherwise NULL is returned
	CRange *PrngExtend(CMemoryPool *mp, CRange *prange);

	// construct scalar expression
	CExpression *PexprScalar(CMemoryPool *mp, const CColRef *colref);

	// is this interval unbounded
	BOOL
	IsConstraintUnbounded() const
	{
		return (NULL == m_pdatumLeft && NULL == m_pdatumRight);
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CRange

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CRange &range)
{
	return range.OsPrint(os);
}
}  // namespace gpopt

#endif
