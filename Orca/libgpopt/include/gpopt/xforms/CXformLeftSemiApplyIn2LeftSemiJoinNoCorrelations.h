//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations.h
//
//	@doc:
//		Turn Left Semi Apply (with IN semantics) into Left Semi join when inner
//		child has no outer references
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations_H
#define GPOPT_CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftSemiApply2LeftSemiJoinNoCorrelations.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations
	: public CXformLeftSemiApply2LeftSemiJoinNoCorrelations
{
private:
	// private copy ctor
	CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations(
		const CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations &);

public:
	// ctor
	explicit CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations(CMemoryPool *mp)
		: CXformLeftSemiApply2LeftSemiJoinNoCorrelations(
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
	virtual ~CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiApplyIn2LeftSemiJoinNoCorrelations;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations";
	}


};	// class CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations_H

// EOF
