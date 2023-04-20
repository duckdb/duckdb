//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformInnerJoin2NLJoin.h
//
//	@doc:
//		Transform inner join to inner NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2NLJoin_H
#define GPOPT_CXformInnerJoin2NLJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2NLJoin
//
//	@doc:
//		Transform inner join to inner NLJ
//
//---------------------------------------------------------------------------
class CXformInnerJoin2NLJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformInnerJoin2NLJoin(const CXformInnerJoin2NLJoin &);

public:
	// ctor
	explicit CXformInnerJoin2NLJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformInnerJoin2NLJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2NLJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2NLJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformInnerJoin2NLJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoin2NLJoin_H

// EOF
