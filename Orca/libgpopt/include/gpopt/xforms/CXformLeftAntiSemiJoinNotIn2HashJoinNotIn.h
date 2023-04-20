//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn.h
//
//	@doc:
//		Transform left anti semi join to left anti semi hash join (NotIn semantics)
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoinNotIn2HashJoinNotIn_H
#define GPOPT_CXformLeftAntiSemiJoinNotIn2HashJoinNotIn_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoinNotIn2HashJoinNotIn
//
//	@doc:
//		Transform left semi join to left anti semi hash join (NotIn semantics)
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoinNotIn2HashJoinNotIn : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoinNotIn2HashJoinNotIn(
		const CXformLeftAntiSemiJoinNotIn2HashJoinNotIn &);

public:
	// ctor
	explicit CXformLeftAntiSemiJoinNotIn2HashJoinNotIn(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftAntiSemiJoinNotIn2HashJoinNotIn()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoinNotIn2HashJoinNotIn;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoinNotIn2HashJoinNotIn";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiJoinNotIn2HashJoinNotIn

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiJoinNotIn2HashJoinNotIn_H

// EOF
