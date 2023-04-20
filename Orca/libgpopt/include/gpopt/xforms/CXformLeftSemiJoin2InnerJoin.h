//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiJoin2InnerJoin.h
//
//	@doc:
//		Transform left semi join to inner join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2InnerJoin_H
#define GPOPT_CXformLeftSemiJoin2InnerJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiJoin2InnerJoin
//
//	@doc:
//		Transform left semi join to inner join
//
//---------------------------------------------------------------------------
class CXformLeftSemiJoin2InnerJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformLeftSemiJoin2InnerJoin(const CXformLeftSemiJoin2InnerJoin &);

public:
	// ctor
	explicit CXformLeftSemiJoin2InnerJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftSemiJoin2InnerJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiJoin2InnerJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiJoin2InnerJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftSemiJoin2InnerJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftSemiJoin2InnerJoin_H

// EOF
