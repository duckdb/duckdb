//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiApply2LeftAntiSemiJoin.h
//
//	@doc:
//		Turn LAS apply into LAS join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiApply2LeftAntiSemiJoin_H
#define GPOPT_CXformLeftAntiSemiApply2LeftAntiSemiJoin_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformApply2Join.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiApply2LeftAntiSemiJoin
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiApply2LeftAntiSemiJoin
	: public CXformApply2Join<CLogicalLeftAntiSemiApply,
							  CLogicalLeftAntiSemiJoin>
{
private:
	// private copy ctor
	CXformLeftAntiSemiApply2LeftAntiSemiJoin(
		const CXformLeftAntiSemiApply2LeftAntiSemiJoin &);

public:
	// ctor
	explicit CXformLeftAntiSemiApply2LeftAntiSemiJoin(CMemoryPool *mp)
		: CXformApply2Join<CLogicalLeftAntiSemiApply, CLogicalLeftAntiSemiJoin>(
			  mp, true /*fDeepTree*/)
	{
	}

	// dtor
	virtual ~CXformLeftAntiSemiApply2LeftAntiSemiJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiApply2LeftAntiSemiJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiApply2LeftAntiSemiJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiApply2LeftAntiSemiJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiApply2LeftAntiSemiJoin_H

// EOF
