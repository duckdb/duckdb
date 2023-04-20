//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin.h
//
//	@doc:
//		Our implementation of LeftOuterJoin (LOJ) can only build on the inner (right) side.
//		If the inner is much larger than the outer side, we can apply this transformation,
//		which computes an inner join and unions it with the outer tuples that do not join:
//
//		Transform
//      LOJ
//        |--Small
//        +--Big
//
// 		to
//
//      UnionAll
//      |---A
//      +---Project_{append nulls)
//          +---LASJ_(key(Small))
//                   |---Small
//                   +---Gb(keys(Small))
//                        +---A
//
//		where A is the InnerJoin(Big, Small)
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin_H
#define GPOPT_CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin : public CXformExploration
{
private:
	// if ratio of the cardinalities outer/inner is below this value, we apply the xform
	static const DOUBLE m_dOuterInnerRatioThreshold;

	// disable copy ctor
	CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin(
		const CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin &);

	// check the stats ratio to decide whether to apply the xform or not
	BOOL FApplyXformUsingStatsInfo(const IStatistics *outer_stats,
								   const IStatistics *inner_side_stats) const;

	// check if the inner expression is of a type which should be considered by this xform
	static BOOL FValidInnerExpr(CExpression *pexprInner);

	// construct a left anti semi join with the CTE consumer (ulCTEJoinId) as outer
	// and a group by as inner
	static CExpression *PexprLeftAntiSemiJoinWithInnerGroupBy(
		CMemoryPool *mp, CColRefArray *pdrgpcrOuter,
		CColRefArray *pdrgpcrOuterCopy, CColRefSet *pcrsScalar,
		CColRefSet *pcrsInner, CColRefArray *pdrgpcrJoinOutput,
		ULONG ulCTEJoinId, ULONG ulCTEOuterId);

	// return a project over a left anti semi join that appends nulls for all
	// columns in the original inner child
	static CExpression *PexprProjectOverLeftAntiSemiJoin(
		CMemoryPool *mp, CColRefArray *pdrgpcrOuter, CColRefSet *pcrsScalar,
		CColRefSet *pcrsInner, CColRefArray *pdrgpcrJoinOutput,
		ULONG ulCTEJoinId, ULONG ulCTEOuterId,
		CColRefArray **ppdrgpcrProjectOutput);

public:
	// ctor
	explicit CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin()
	{
	}

	// identifier
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuter2InnerUnionAllLeftAntiSemiJoin;
	}

	// return a string for the xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// do stats need to be computed before applying xform?
	virtual BOOL
	FNeedsStats() const
	{
		return true;
	}

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

	// return true if xform should be applied only once
	virtual BOOL IsApplyOnce();
};
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin_H

// EOF
