//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn.h
//
//	@doc:
//		Transform left anti semi join to left anti semi NLJ (NotIn semantics)
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoinNotIn2NLJoinNotIn_H
#define GPOPT_CXformLeftAntiSemiJoinNotIn2NLJoinNotIn_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoinNotIn2NLJoinNotIn
//
//	@doc:
//		Transform left anti semi join to left anti semi NLJ (NotIn semantics)
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoinNotIn2NLJoinNotIn : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoinNotIn2NLJoinNotIn(
		const CXformLeftAntiSemiJoinNotIn2NLJoinNotIn &);

public:
	// ctor
	explicit CXformLeftAntiSemiJoinNotIn2NLJoinNotIn(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftAntiSemiJoinNotIn2NLJoinNotIn()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoinNotIn2NLJoinNotIn;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoinNotIn2NLJoinNotIn";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiJoinNotIn2NLJoinNotIn

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiJoinNotIn2NLJoinNotIn_H

// EOF
