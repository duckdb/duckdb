//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn.h
//
//	@doc:
//		Turn LAS apply into LAS join (NotIn semantics)
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn_H
#define GPOPT_CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformApply2Join.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn
	: public CXformApply2Join<CLogicalLeftAntiSemiApplyNotIn,
							  CLogicalLeftAntiSemiJoinNotIn>
{
private:
	// private copy ctor
	CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn(
		const CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn &);

public:
	// ctor
	explicit CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn(CMemoryPool *mp)
		: CXformApply2Join<CLogicalLeftAntiSemiApplyNotIn,
						   CLogicalLeftAntiSemiJoinNotIn>(mp,
														  true /*fDeepTree*/)
	{
	}

	// dtor
	virtual ~CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn_H

// EOF
