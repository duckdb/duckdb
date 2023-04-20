//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoin2NLJoin.h
//
//	@doc:
//		Transform left anti semi join to left anti semi NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2NLJoin_H
#define GPOPT_CXformLeftAntiSemiJoin2NLJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoin2NLJoin
//
//	@doc:
//		Transform left anti semi join to left anti semi NLJ
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoin2NLJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoin2NLJoin(const CXformLeftAntiSemiJoin2NLJoin &);


public:
	// ctor
	explicit CXformLeftAntiSemiJoin2NLJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftAntiSemiJoin2NLJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoin2NLJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoin2NLJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiJoin2NLJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftAntiSemiJoin2NLJoin_H

// EOF
