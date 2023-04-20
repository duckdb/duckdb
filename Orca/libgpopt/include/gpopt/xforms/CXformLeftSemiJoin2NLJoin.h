//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftSemiJoin2NLJoin.h
//
//	@doc:
//		Transform left semi join to left semi NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2NLJoin_H
#define GPOPT_CXformLeftSemiJoin2NLJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiJoin2NLJoin
//
//	@doc:
//		Transform left semi join to left semi NLJ
//
//---------------------------------------------------------------------------
class CXformLeftSemiJoin2NLJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftSemiJoin2NLJoin(const CXformLeftSemiJoin2NLJoin &);

public:
	// ctor
	explicit CXformLeftSemiJoin2NLJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftSemiJoin2NLJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiJoin2NLJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiJoin2NLJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftSemiJoin2NLJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftSemiJoin2NLJoin_H

// EOF
