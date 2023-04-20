//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIntersectAll2LeftSemiJoin.h
//
//	@doc:
//		Class to transform of CLogicalIntersectAll into a left semi join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformIntersectAll2LeftSemiJoin_H
#define GPOPT_CXformIntersectAll2LeftSemiJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformIntersectAll2LeftSemiJoin
//
//	@doc:
//		Class to transform of CLogicalIntersectAll into a left semi join
//
//---------------------------------------------------------------------------
class CXformIntersectAll2LeftSemiJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformIntersectAll2LeftSemiJoin(const CXformIntersectAll2LeftSemiJoin &);

public:
	// ctor
	explicit CXformIntersectAll2LeftSemiJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformIntersectAll2LeftSemiJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfIntersectAll2LeftSemiJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformIntersectAll2LeftSemiJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformIntersectAll2LeftSemiJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformIntersectAll2LeftSemiJoin_H

// EOF
