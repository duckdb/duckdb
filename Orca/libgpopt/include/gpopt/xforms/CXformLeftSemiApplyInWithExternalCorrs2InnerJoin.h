//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformLeftSemiApplyInWithExternalCorrs2InnerJoin.h
//
//	@doc:
//		Turn Left Semi Apply (with IN semantics) with external correlations
//		into inner join;
//		external correlations are correlations in the inner child of LSA
//		that use columns not defined by the outer child of LSA
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H
#define GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftSemiApplyWithExternalCorrs2InnerJoin.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiApplyInWithExternalCorrs2InnerJoin
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformLeftSemiApplyInWithExternalCorrs2InnerJoin
	: public CXformLeftSemiApplyWithExternalCorrs2InnerJoin
{
private:
	// private copy ctor
	CXformLeftSemiApplyInWithExternalCorrs2InnerJoin(
		const CXformLeftSemiApplyInWithExternalCorrs2InnerJoin &);

public:
	// ctor
	explicit CXformLeftSemiApplyInWithExternalCorrs2InnerJoin(CMemoryPool *mp)
		: CXformLeftSemiApplyWithExternalCorrs2InnerJoin(
			  mp, GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CLogicalLeftSemiApplyIn(mp),
					  GPOS_NEW(mp) CExpression(
						  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
					  GPOS_NEW(mp) CExpression(
						  mp, GPOS_NEW(mp) CPatternTree(mp)),  // right child
					  GPOS_NEW(mp) CExpression(
						  mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
					  ))
	{
	}

	// dtor
	virtual ~CXformLeftSemiApplyInWithExternalCorrs2InnerJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiApplyInWithExternalCorrs2InnerJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiApplyInWithExternalCorrs2InnerJoin";
	}


};	// class CXformLeftSemiApplyInWithExternalCorrs2InnerJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftSemiApplyInWithExternalCorrs2InnerJoin_H

// EOF
