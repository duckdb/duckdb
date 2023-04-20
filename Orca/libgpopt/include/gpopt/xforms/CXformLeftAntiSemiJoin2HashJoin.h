//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoin2HashJoin.h
//
//	@doc:
//		Transform left anti semi join to left anti semi hash join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2HashJoin_H
#define GPOPT_CXformLeftAntiSemiJoin2HashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoin2HashJoin
//
//	@doc:
//		Transform left semi join to left anti semi hash join
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoin2HashJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoin2HashJoin(const CXformLeftAntiSemiJoin2HashJoin &);

public:
	// ctor
	explicit CXformLeftAntiSemiJoin2HashJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftAntiSemiJoin2HashJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoin2HashJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoin2HashJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiJoin2HashJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftAntiSemiJoin2HashJoin_H

// EOF
